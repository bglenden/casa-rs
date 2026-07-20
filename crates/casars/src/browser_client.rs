// SPDX-License-Identifier: LGPL-3.0-or-later
use std::io::{BufRead, BufReader, Write};
use std::marker::PhantomData;
use std::process::{Child, ChildStdin, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use casa_provider_contracts::{VersionedSessionEnvelope, encode_session_message};
use casars_imagebrowser_protocol::{
    ImageBrowserCommand, ImageBrowserPreviewPayload, ImageBrowserRequestEnvelope,
    ImageBrowserResponse, ImageBrowserResponseEnvelope, ImageBrowserSnapshot,
    PROTOCOL_VERSION as IMAGE_BROWSER_PROTOCOL_VERSION,
};
use casars_tablebrowser_protocol::{
    BrowserCommand, BrowserRequestEnvelope, BrowserResponse, BrowserResponseEnvelope,
    BrowserSnapshot, PROTOCOL_VERSION as TABLE_BROWSER_PROTOCOL_VERSION,
};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::registry::ResolvedCommand;

const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_STARTUP_TIMEOUT_MS: u64 = 120_000;

#[derive(Debug, Clone)]
pub(crate) enum SessionRequestError {
    Provider { code: String, message: String },
    Serialization(String),
    MalformedResponse(String),
    ProtocolVersion { expected: u32, actual: u32 },
    UnexpectedResponse(String),
    Timeout(String),
    ProcessExit(String),
    Configuration(String),
    Transport(String),
}

impl SessionRequestError {
    pub(crate) fn message(&self) -> &str {
        match self {
            Self::Provider { message, .. }
            | Self::Serialization(message)
            | Self::MalformedResponse(message)
            | Self::UnexpectedResponse(message)
            | Self::Timeout(message)
            | Self::ProcessExit(message)
            | Self::Configuration(message)
            | Self::Transport(message) => message,
            Self::ProtocolVersion { .. } => "session protocol version mismatch",
        }
    }

    pub(crate) fn is_transport(&self) -> bool {
        !matches!(self, Self::Provider { .. })
    }

    #[cfg(test)]
    fn contains(&self, needle: &str) -> bool {
        self.to_string().contains(needle)
    }
}

impl std::fmt::Display for SessionRequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Provider { code, message } => write!(formatter, "{code}: {message}"),
            Self::MalformedResponse(message) => {
                write!(formatter, "invalid_response: {message}")
            }
            Self::ProtocolVersion { expected, actual } => write!(
                formatter,
                "session protocol version mismatch: expected {expected}, received {actual}"
            ),
            _ => formatter.write_str(self.message()),
        }
    }
}

impl std::error::Error for SessionRequestError {}

pub(crate) trait JsonlSessionProtocol {
    type Command;
    type Request: Serialize;
    type Response: DeserializeOwned + VersionedSessionEnvelope;

    const NAME: &'static str;
    const VERSION: u32;

    fn request(command: Self::Command) -> Self::Request;
}

#[derive(Debug)]
pub(crate) struct TableBrowserProtocol;

#[derive(Debug)]
pub(crate) struct ImageBrowserProtocol;

impl JsonlSessionProtocol for TableBrowserProtocol {
    type Command = BrowserCommand;
    type Request = BrowserRequestEnvelope;
    type Response = BrowserResponseEnvelope;

    const NAME: &'static str = "tablebrowser";
    const VERSION: u32 = TABLE_BROWSER_PROTOCOL_VERSION;

    fn request(command: Self::Command) -> Self::Request {
        BrowserRequestEnvelope::new(command)
    }
}

impl JsonlSessionProtocol for ImageBrowserProtocol {
    type Command = ImageBrowserCommand;
    type Request = ImageBrowserRequestEnvelope;
    type Response = ImageBrowserResponseEnvelope;

    const NAME: &'static str = "imexplore";
    const VERSION: u32 = IMAGE_BROWSER_PROTOCOL_VERSION;

    fn request(command: Self::Command) -> Self::Request {
        ImageBrowserRequestEnvelope::new(command)
    }
}

#[derive(Debug, Clone, Copy)]
struct SessionTimeouts {
    request: Duration,
    startup: Duration,
}

impl SessionTimeouts {
    fn from_env() -> Result<Self, SessionRequestError> {
        Ok(Self {
            request: duration_from_env(
                "CASARS_BROWSER_REQUEST_TIMEOUT_MS",
                DEFAULT_REQUEST_TIMEOUT_MS,
            )?,
            startup: duration_from_env(
                "CASARS_BROWSER_STARTUP_TIMEOUT_MS",
                DEFAULT_STARTUP_TIMEOUT_MS,
            )?,
        })
    }
}

#[derive(Debug)]
pub(crate) struct JsonlSessionClient<P> {
    process: SessionProcessClient,
    timeouts: SessionTimeouts,
    protocol: PhantomData<P>,
}

#[derive(Debug)]
struct SessionProcessClient {
    child: Arc<Mutex<Child>>,
    stdin: Arc<Mutex<ChildStdin>>,
    responses: Receiver<String>,
    stderr: Arc<Mutex<String>>,
    stderr_closed: Receiver<()>,
}

impl SessionProcessClient {
    fn spawn(command: &ResolvedCommand, session_name: &str) -> Result<Self, SessionRequestError> {
        let mut process = command.command();
        process
            .arg("--session")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = process.spawn().map_err(|error| {
            SessionRequestError::Transport(format!("spawn {session_name} session: {error}"))
        })?;
        let stdin = child.stdin.take().ok_or_else(|| {
            SessionRequestError::Transport(format!("{session_name} session stdin was not captured"))
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            SessionRequestError::Transport(format!(
                "{session_name} session stdout was not captured"
            ))
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            SessionRequestError::Transport(format!(
                "{session_name} session stderr was not captured"
            ))
        })?;

        let child = Arc::new(Mutex::new(child));
        let stdin = Arc::new(Mutex::new(stdin));
        let stderr_buffer = Arc::new(Mutex::new(String::new()));
        let (tx, rx) = mpsc::channel();
        let (stderr_done_tx, stderr_done_rx) = mpsc::channel();

        thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line_result in reader.lines() {
                let Ok(line) = line_result else {
                    break;
                };
                if line.trim().is_empty() {
                    continue;
                }
                if tx.send(line).is_err() {
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
            let _ = stderr_done_tx.send(());
        });

        Ok(Self {
            child,
            stdin,
            responses: rx,
            stderr: stderr_buffer,
            stderr_closed: stderr_done_rx,
        })
    }

    fn request_raw(
        &self,
        payload: &str,
        timeout: Duration,
        session_name: &str,
    ) -> Result<String, SessionRequestError> {
        {
            let mut stdin = self.stdin.lock().map_err(|_| {
                SessionRequestError::Transport("failed to acquire browser stdin lock".to_string())
            })?;
            stdin
                .write_all(payload.as_bytes())
                .and_then(|_| stdin.write_all(b"\n"))
                .and_then(|_| stdin.flush())
                .map_err(|error| {
                    SessionRequestError::Transport(format!("write {session_name} request: {error}"))
                })?;
        }

        self.responses
            .recv_timeout(timeout)
            .map_err(|error| match error {
                RecvTimeoutError::Timeout => {
                    let _ = self.terminate_and_wait();
                    SessionRequestError::Timeout(format_browser_failure(
                        &format!("timed out waiting for {session_name} response"),
                        self.stderr_text_after_drain(),
                        None,
                    ))
                }
                RecvTimeoutError::Disconnected => {
                    let status = self.reap_exit_status();
                    SessionRequestError::ProcessExit(format_browser_failure(
                        &format!("{session_name} session exited"),
                        self.stderr_text_after_drain(),
                        status,
                    ))
                }
            })
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

    fn stderr_text_after_drain(&self) -> String {
        let _ = self.stderr_closed.recv_timeout(Duration::from_millis(50));
        self.stderr_text()
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

impl<P> Drop for JsonlSessionClient<P> {
    fn drop(&mut self) {
        let _ = self.process.terminate_and_wait();
    }
}

impl<P> JsonlSessionClient<P>
where
    P: JsonlSessionProtocol,
{
    pub(crate) fn spawn(command: &ResolvedCommand) -> Result<Self, SessionRequestError> {
        let timeouts = SessionTimeouts::from_env()?;
        let process = SessionProcessClient::spawn(command, P::NAME)?;
        Ok(Self {
            process,
            timeouts,
            protocol: PhantomData,
        })
    }

    pub(crate) fn cancel(&self) -> Result<(), String> {
        self.process.cancel()
    }

    pub(crate) fn stderr_text(&self) -> String {
        self.process.stderr_text()
    }

    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self {
            process: SessionProcessClient::spawn_stub(P::NAME),
            timeouts: SessionTimeouts {
                request: Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS),
                startup: Duration::from_millis(DEFAULT_STARTUP_TIMEOUT_MS),
            },
            protocol: PhantomData,
        }
    }

    fn exchange(
        &self,
        command: P::Command,
        timeout: Duration,
    ) -> Result<P::Response, SessionRequestError> {
        let payload = encode_session_message(&P::request(command)).map_err(|error| {
            SessionRequestError::Serialization(format!("serialize {} request: {error}", P::NAME))
        })?;
        let line = self.process.request_raw(&payload, timeout, P::NAME)?;
        let response = serde_json::from_str::<P::Response>(&line).map_err(|error| {
            SessionRequestError::MalformedResponse(format!("parse {} response: {error}", P::NAME))
        })?;
        let actual = response.protocol_version();
        if actual != P::VERSION {
            return Err(SessionRequestError::ProtocolVersion {
                expected: P::VERSION,
                actual,
            });
        }
        Ok(response)
    }
}

impl JsonlSessionClient<TableBrowserProtocol> {
    pub(crate) fn request(
        &self,
        command: BrowserCommand,
    ) -> Result<BrowserSnapshot, SessionRequestError> {
        self.request_with_timeout(command, self.timeouts.request)
    }

    pub(crate) fn request_startup(
        &self,
        command: BrowserCommand,
    ) -> Result<BrowserSnapshot, SessionRequestError> {
        self.request_with_timeout(command, self.timeouts.startup)
    }

    fn request_with_timeout(
        &self,
        command: BrowserCommand,
        timeout: Duration,
    ) -> Result<BrowserSnapshot, SessionRequestError> {
        let response = self.exchange(command, timeout)?;
        match response.response {
            BrowserResponse::Snapshot(snapshot) => Ok(*snapshot),
            BrowserResponse::Error(error) => Err(SessionRequestError::Provider {
                code: error.code,
                message: error.message,
            }),
        }
    }
}

impl JsonlSessionClient<ImageBrowserProtocol> {
    pub(crate) fn request(
        &self,
        command: ImageBrowserCommand,
    ) -> Result<ImageBrowserSnapshot, SessionRequestError> {
        self.request_with_timeout(command, self.timeouts.request)
    }

    pub(crate) fn request_startup(
        &self,
        command: ImageBrowserCommand,
    ) -> Result<ImageBrowserSnapshot, SessionRequestError> {
        self.request_with_timeout(command, self.timeouts.startup)
    }

    pub(crate) fn request_preview(
        &self,
        command: ImageBrowserCommand,
    ) -> Result<ImageBrowserPreviewPayload, SessionRequestError> {
        self.request_preview_with_timeout(command, self.timeouts.request)
    }

    fn request_with_timeout(
        &self,
        command: ImageBrowserCommand,
        timeout: Duration,
    ) -> Result<ImageBrowserSnapshot, SessionRequestError> {
        let response = self.exchange(command, timeout)?;
        match response.response {
            ImageBrowserResponse::Snapshot(snapshot) => Ok(*snapshot),
            ImageBrowserResponse::Preview(_) => Err(SessionRequestError::UnexpectedResponse(
                "unexpected imexplore preview response for snapshot request".to_string(),
            )),
            ImageBrowserResponse::Error(error) => Err(SessionRequestError::Provider {
                code: error.code,
                message: error.message,
            }),
        }
    }

    fn request_preview_with_timeout(
        &self,
        command: ImageBrowserCommand,
        timeout: Duration,
    ) -> Result<ImageBrowserPreviewPayload, SessionRequestError> {
        let response = self.exchange(command, timeout)?;
        match response.response {
            ImageBrowserResponse::Preview(preview) => Ok(*preview),
            ImageBrowserResponse::Snapshot(_) => Err(SessionRequestError::UnexpectedResponse(
                "unexpected imexplore snapshot response for preview request".to_string(),
            )),
            ImageBrowserResponse::Error(error) => Err(SessionRequestError::Provider {
                code: error.code,
                message: error.message,
            }),
        }
    }
}

#[cfg(test)]
impl SessionProcessClient {
    fn spawn_stub(session_name: &str) -> Self {
        let mut child = std::process::Command::new("cat")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|error| panic!("spawn {session_name} stub session: {error}"));
        let stdin = child
            .stdin
            .take()
            .unwrap_or_else(|| panic!("{session_name} stub stdin was not captured"));
        let stdout = child
            .stdout
            .take()
            .unwrap_or_else(|| panic!("{session_name} stub stdout was not captured"));
        let stderr = child
            .stderr
            .take()
            .unwrap_or_else(|| panic!("{session_name} stub stderr was not captured"));

        let child = Arc::new(Mutex::new(child));
        let stdin = Arc::new(Mutex::new(stdin));
        let stderr_buffer = Arc::new(Mutex::new(String::new()));
        let (tx, rx) = mpsc::channel();
        let (stderr_done_tx, stderr_done_rx) = mpsc::channel();

        thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line_result in reader.lines() {
                let Ok(line) = line_result else {
                    break;
                };
                if line.trim().is_empty() {
                    continue;
                }
                if tx.send(line).is_err() {
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
            let _ = stderr_done_tx.send(());
        });

        Self {
            child,
            stdin,
            responses: rx,
            stderr: stderr_buffer,
            stderr_closed: stderr_done_rx,
        }
    }
}

fn duration_from_env(name: &str, default_ms: u64) -> Result<Duration, SessionRequestError> {
    let Some(value) = std::env::var_os(name) else {
        return Ok(Duration::from_millis(default_ms));
    };
    let text = value.into_string().map_err(|_| {
        SessionRequestError::Configuration(format!(
            "{name} must contain a positive UTF-8 integer number of milliseconds"
        ))
    })?;
    let millis = text.parse::<u64>().map_err(|error| {
        SessionRequestError::Configuration(format!("invalid {name} value {text:?}: {error}"))
    })?;
    if millis == 0 {
        return Err(SessionRequestError::Configuration(format!(
            "invalid {name} value {text:?}: timeout must be positive"
        )));
    }
    Ok(Duration::from_millis(millis))
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
    use std::thread;

    use casars_tablebrowser_protocol::{
        BrowserCommand, BrowserResponseEnvelope, BrowserSnapshot, BrowserViewport,
    };
    use tempfile::tempdir;

    use super::*;
    #[test]
    fn timeout_terminates_session_before_a_late_reply_can_be_reused() {
        let _guard = crate::test_env_lock();
        let temp = tempdir().expect("tempdir");
        let script = write_slow_browser_script(temp.path(), 200);
        unsafe {
            std::env::set_var("CASARS_BROWSER_REQUEST_TIMEOUT_MS", "50");
        }

        let client = spawn_test_browser(script);

        let error = client
            .request(BrowserCommand::GetSnapshot { viewport: None })
            .expect_err("request should time out");
        assert!(error.contains("timed out waiting for"));

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

    #[test]
    fn invalid_json_response_is_reported_as_protocol_error() {
        let _guard = crate::test_env_lock();
        let temp = tempdir().expect("tempdir");
        let script = write_browser_script(
            temp.path(),
            "#!/bin/sh\nwhile IFS= read -r _line; do\n  printf '{not-json}\\n'\ndone\n",
        );
        let client = spawn_test_browser(script);

        let error = client
            .request(BrowserCommand::GetSnapshot { viewport: None })
            .expect_err("invalid response should fail");
        assert!(matches!(error, SessionRequestError::MalformedResponse(_)));
        assert!(error.contains("parse tablebrowser response"));
    }

    #[test]
    fn response_version_mismatch_is_distinct_from_provider_errors() {
        let _guard = crate::test_env_lock();
        let temp = tempdir().expect("tempdir");
        let mut response =
            serde_json::to_value(BrowserResponseEnvelope::snapshot(empty_browser_snapshot()))
                .expect("serialize response");
        response["version"] = serde_json::json!(TABLE_BROWSER_PROTOCOL_VERSION + 1);
        let script = write_browser_script(
            temp.path(),
            &format!(
                "#!/bin/sh\nwhile IFS= read -r _line; do\n  printf '%s\\n' '{}'\ndone\n",
                serde_json::to_string(&response).expect("encode response")
            ),
        );
        let client = spawn_test_browser(script);

        let error = client
            .request(BrowserCommand::GetSnapshot { viewport: None })
            .expect_err("version mismatch should fail");
        assert!(matches!(
            error,
            SessionRequestError::ProtocolVersion {
                expected: TABLE_BROWSER_PROTOCOL_VERSION,
                actual
            } if actual == TABLE_BROWSER_PROTOCOL_VERSION + 1
        ));
    }

    #[test]
    fn invalid_timeout_configuration_is_rejected_before_spawn() {
        let _guard = crate::test_env_lock();
        unsafe {
            std::env::set_var("CASARS_BROWSER_REQUEST_TIMEOUT_MS", "not-a-duration");
        }
        let result = JsonlSessionClient::<TableBrowserProtocol>::spawn(&ResolvedCommand::direct(
            PathBuf::from("/usr/bin/true"),
        ));
        unsafe {
            std::env::remove_var("CASARS_BROWSER_REQUEST_TIMEOUT_MS");
        }
        let error = result.expect_err("invalid timeout must fail");
        assert!(matches!(error, SessionRequestError::Configuration(_)));
        assert!(error.contains("CASARS_BROWSER_REQUEST_TIMEOUT_MS"));
    }

    #[test]
    fn disconnected_session_surfaces_exit_and_stderr() {
        let _guard = crate::test_env_lock();
        let temp = tempdir().expect("tempdir");
        let script = write_browser_script(
            temp.path(),
            "#!/bin/sh\nwhile IFS= read -r _line; do\n  echo 'backend exploded' >&2\n  exit 7\ndone\n",
        );
        let client = spawn_test_browser(script);

        let error = client
            .request(BrowserCommand::GetSnapshot { viewport: None })
            .expect_err("disconnected session should fail");
        assert!(
            error.contains("tablebrowser session exited")
                || error.contains("write browser request")
                || error.contains("timed out waiting for browser response")
        );
        assert!(error.contains("backend exploded") || error.contains("exit status: 7"));
    }

    fn write_slow_browser_script(root: &Path, delay_ms: u64) -> PathBuf {
        let response =
            serde_json::to_string(&BrowserResponseEnvelope::snapshot(empty_browser_snapshot()))
                .expect("serialize snapshot");
        let script = format!(
            "#!/bin/sh\nwhile IFS= read -r line; do\n  sleep {}\n  printf '%s\\n' '{}'\ndone\n",
            delay_ms as f64 / 1000.0,
            response
        );
        write_browser_script(root, &script)
    }

    fn empty_browser_snapshot() -> BrowserSnapshot {
        BrowserSnapshot {
            capabilities: casars_tablebrowser_protocol::BrowserCapabilities { editable: false },
            view: casars_tablebrowser_protocol::BrowserView::Overview,
            parameters: casars_tablebrowser_protocol::BrowserParameters::default(),
            focus: casars_tablebrowser_protocol::BrowserFocus::Main,
            table_path: "/tmp/fake.ms".to_string(),
            breadcrumb: Vec::new(),
            viewport: BrowserViewport::new(80, 24),
            status_line: "ok".to_string(),
            content_lines: vec!["Overview".to_string()],
            vertical_metrics: None,
            horizontal_metrics: None,
            selected_address: None,
            inspector: None,
        }
    }

    fn spawn_test_browser(script: PathBuf) -> JsonlSessionClient<TableBrowserProtocol> {
        for attempt in 0..3 {
            match JsonlSessionClient::<TableBrowserProtocol>::spawn(&ResolvedCommand::direct(
                script.clone(),
            )) {
                Ok(client) => return client,
                Err(error) if error.contains("Text file busy") && attempt < 2 => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("spawn browser client: {error}"),
            }
        }
        unreachable!("test browser spawn loop always returns or panics")
    }

    fn write_browser_script(root: &Path, script: &str) -> PathBuf {
        let path = root.join("browser.sh");
        fs::write(&path, script).expect("write script");
        let mut permissions = fs::metadata(&path).expect("metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&path, permissions).expect("chmod");
        path
    }
}
