// SPDX-License-Identifier: LGPL-3.0-or-later
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use casacore_tablebrowser_protocol::{
    BrowserCommand, BrowserRequestEnvelope, BrowserResponse, BrowserResponseEnvelope,
    BrowserSnapshot,
};

use crate::registry::ResolvedCommand;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

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
            .recv_timeout(REQUEST_TIMEOUT)
            .map_err(|error| {
                let stderr = self.stderr_text();
                let exit = self
                    .child
                    .lock()
                    .ok()
                    .and_then(|mut child| child.try_wait().ok().flatten());
                match exit {
                    Some(status) => format!(
                        "tablebrowser session exited with {}{}",
                        status,
                        if stderr.trim().is_empty() {
                            String::new()
                        } else {
                            format!(": {}", stderr.trim())
                        }
                    ),
                    None => format!(
                        "timed out waiting for browser response ({error}){}",
                        if stderr.trim().is_empty() {
                            String::new()
                        } else {
                            format!(": {}", stderr.trim())
                        }
                    ),
                }
            })?;

        match response.response {
            BrowserResponse::Snapshot(snapshot) => Ok(*snapshot),
            BrowserResponse::Error(error) => Err(format!("{}: {}", error.code, error.message)),
        }
    }

    pub(crate) fn cancel(&self) -> Result<(), String> {
        self.child
            .lock()
            .map_err(|_| "failed to acquire browser child lock".to_string())?
            .kill()
            .map_err(|error| format!("terminate browser session: {error}"))
    }

    pub(crate) fn stderr_text(&self) -> String {
        self.stderr
            .lock()
            .map(|stderr| stderr.clone())
            .unwrap_or_default()
    }
}
