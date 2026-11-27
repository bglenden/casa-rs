// SPDX-License-Identifier: LGPL-3.0-or-later
//! Python signal delivery and live log forwarding for native task runs.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, SyncSender, channel, sync_channel};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use casa_contracts::{TaskFailureKind, TaskReply};
use casa_tables::log_table::{
    CasaLogLayer, CasaLogSink, CasaPriority, CasaTableLogSink, CompositeSink, TableLogOpenMode,
    WriterSink,
};
use casa_tasks::{NativeTaskOptions, TaskCancellation, run_task_in_process};
use pyo3::exceptions::{PyKeyboardInterrupt, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tracing_subscriber::{Registry, layer::SubscriberExt};

use crate::convert::reply_value::reply_value;
use crate::parameters::{parameter_values, reason_error, to_python};
use crate::tasks::{TaskTableWrite, completion, decode_plot_documents};

#[derive(FromPyObject)]
#[pyo3(from_item_all)]
pub(crate) struct PythonTaskOptions {
    argument_text: BTreeMap<String, String>,
    plot_payload: Option<PathBuf>,
    applycal_value_only: bool,
    bandpass_completed_pages: Option<usize>,
    #[cfg(unix)]
    table_write: Option<Py<TaskTableWrite>>,
}

/// A native reply retained through Python-side rendering and product checks.
#[pyclass(frozen)]
pub(crate) struct NativeTaskResult {
    reply: TaskReply,
    #[pyo3(get)]
    logs: String,
}

#[pymethods]
impl NativeTaskResult {
    #[getter]
    fn status(&self) -> &'static str {
        match &self.reply {
            TaskReply::Succeeded { .. } => "succeeded",
            TaskReply::Failed { kind, .. } => match kind {
                TaskFailureKind::Validation => "refused",
                TaskFailureKind::Execution => "failed",
                TaskFailureKind::Interrupted => "interrupted",
            },
        }
    }

    #[getter]
    fn message(&self) -> Option<&str> {
        match &self.reply {
            TaskReply::Failed { message, .. } => Some(message),
            _ => None,
        }
    }

    #[getter]
    fn parameter(&self) -> Option<&str> {
        match &self.reply {
            TaskReply::Failed { parameter, .. } => parameter.as_deref(),
            _ => None,
        }
    }

    fn value<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match &self.reply {
            TaskReply::Succeeded { value, .. } => {
                reply_value(py, value).map(|value| value.into_bound(py))
            }
            _ => Ok(py.None().into_bound(py)),
        }
    }

    fn details<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match &self.reply {
            TaskReply::Succeeded { result, .. } => to_python(py, result),
            _ => Ok(py.None().into_bound(py)),
        }
    }

    fn plots<'py>(&self, py: Python<'py>, payload_bytes: u64) -> PyResult<Bound<'py, PyAny>> {
        let plots = match &self.reply {
            TaskReply::Succeeded {
                surface, result, ..
            } => py
                .detach(|| decode_plot_documents(surface, result.clone(), payload_bytes))
                .map_err(|reason| reason_error(py, reason))?,
            _ => Vec::new(),
        };
        to_python(py, &plots)
    }

    fn completion<'py>(
        &self,
        py: Python<'py>,
        workspace: PathBuf,
        values: Vec<(String, String)>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let TaskReply::Succeeded {
            surface, result, ..
        } = &self.reply
        else {
            return Ok(py.None().into_bound(py));
        };
        let projection = py
            .detach(|| {
                let result = serde_json::to_string(result).map_err(|error| error.to_string())?;
                completion(surface, &result, &workspace, values)
            })
            .map_err(|reason| reason_error(py, reason))?;
        to_python(py, &projection)
    }
}

enum WorkerMessage {
    Log(String, Option<Sender<()>>),
    Finished,
}

struct LogWriter {
    sender: SyncSender<WorkerMessage>,
    synchronize: bool,
}

impl io::Write for LogWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        // With verbose diagnostics enabled, let Python handle the log and
        // any signal it triggers before the worker passes the checkpoint.
        let (acknowledge, delivered) = self.synchronize.then(channel).unzip();
        let line = String::from_utf8_lossy(bytes).into_owned();
        self.sender
            .send(WorkerMessage::Log(line, acknowledge))
            .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;
        if let Some(delivered) = delivered {
            let _ = delivered.recv();
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct TaskLogging {
    table: Option<(PathBuf, CasaPriority)>,
    stderr_priority: Option<CasaPriority>,
    default_output: bool,
}

impl TaskLogging {
    fn read(workspace: &Path) -> PyResult<Self> {
        let path = std::env::var_os("CASA_RS_LOG_TABLE");
        let stderr = std::env::var("CASA_RS_LOG_STDERR_PRIORITY").ok();
        let default_output = path.is_none() && stderr.is_none();
        let stderr_priority = match stderr.as_deref() {
            Some(value) if value.eq_ignore_ascii_case("off") => None,
            Some(value) => Some(parse_priority("CASA_RS_LOG_STDERR_PRIORITY", value)?),
            None => Some(CasaPriority::Warn),
        };
        let table_priority = match std::env::var("CASA_RS_LOG_TABLE_PRIORITY") {
            Ok(value) => parse_priority("CASA_RS_LOG_TABLE_PRIORITY", &value)?,
            Err(_) => CasaPriority::Info,
        };
        Ok(Self {
            table: path.map(|path| (workspace.join(path), table_priority)),
            stderr_priority,
            default_output,
        })
    }

    fn sink(self, sender: SyncSender<WorkerMessage>) -> Arc<dyn CasaLogSink> {
        let mut sinks: Vec<Arc<dyn CasaLogSink>> = Vec::new();
        if let Some((path, priority)) = self.table {
            sinks.push(Arc::new(CasaTableLogSink::new(
                path,
                priority,
                TableLogOpenMode::CreateOrAppend,
            )));
        }
        if let Some(priority) = self.stderr_priority {
            sinks.push(Arc::new(WriterSink::new(
                Arc::new(Mutex::new(LogWriter {
                    sender,
                    synchronize: priority <= CasaPriority::Debug1,
                })),
                priority,
            )));
        }
        Arc::new(CompositeSink::new(sinks))
    }
}

fn parse_priority(variable: &str, text: &str) -> PyResult<CasaPriority> {
    text.parse().map_err(|_| {
        pyo3::exceptions::PyValueError::new_err(format!("invalid {variable} priority {text:?}"))
    })
}

#[pyfunction]
pub(crate) fn run_native_task(
    py: Python<'_>,
    surface: String,
    workspace: PathBuf,
    values: &Bound<'_, PyDict>,
    originals: &Bound<'_, PyDict>,
    options: PythonTaskOptions,
) -> PyResult<NativeTaskResult> {
    py.check_signals()?;
    let values = parameter_values(values)?;
    let originals = parameter_values(originals)?;
    let sys = py.import("sys")?;
    let logging = TaskLogging::read(&workspace)?;
    let (sender, mut receiver) = sync_channel(64);
    let diagnostic_lines = sender.clone();
    let execution = NativeTaskOptions {
        default_logs: logging.default_output,
        log_line: Some(Arc::new(move |line| {
            let _ = diagnostic_lines.send(WorkerMessage::Log(format!("{line}\n"), None));
        })),
        plot_payload: options.plot_payload,
        bandpass_completed_pages: options.bandpass_completed_pages,
        applycal_value_only: options.applycal_value_only,
        listcal_paged_reply: surface == "listcal",
        #[cfg(unix)]
        table_writes: options
            .table_write
            .as_ref()
            .and_then(|write| write.borrow(py).reservations.clone()),
        ..NativeTaskOptions::default()
    };
    let cancel = execution.cancel.clone();
    let worker = std::thread::Builder::new()
        .name(format!("casars-{surface}"))
        .spawn(move || {
            let sink = logging.sink(sender.clone());
            let subscriber = Registry::default().with(CasaLogLayer::filtered(sink.clone()));
            let (reply, lines) = tracing::subscriber::with_default(subscriber, || {
                run_task_in_process(
                    &surface,
                    values,
                    &originals,
                    &options.argument_text,
                    &workspace,
                    &execution,
                )
            });
            for line in lines {
                if sender.send(WorkerMessage::Log(line + "\n", None)).is_err() {
                    break;
                }
            }
            if let Err(error) = sink.flush() {
                let _ = sender.send(WorkerMessage::Log(
                    format!("Error: failed to flush logging: {error}\n"),
                    None,
                ));
            }
            let _ = sender.send(WorkerMessage::Finished);
            reply
        })
        .map_err(|error| {
            PyRuntimeError::new_err(format!("cannot start native task worker: {error}"))
        })?;
    let (logs, interrupted) = collect_logs(py, sys.as_any(), &mut receiver, &cancel, &worker);
    let reply = py.detach(move || worker.join());
    if let Some(error) = interrupted {
        return Err(error);
    }
    let reply = reply.map_err(|_| PyRuntimeError::new_err("native task worker panicked"))?;
    Ok(NativeTaskResult { reply, logs })
}

fn collect_logs(
    py: Python<'_>,
    sys: &Bound<'_, PyAny>,
    receiver: &mut Receiver<WorkerMessage>,
    cancel: &TaskCancellation,
    worker: &std::thread::JoinHandle<TaskReply>,
) -> (String, Option<PyErr>) {
    let mut interrupted = None;
    let mut logs = String::new();
    loop {
        if let Err(error) = py.check_signals() {
            cancel.cancel();
            interrupted.get_or_insert(error);
        }
        let receiver = &mut *receiver;
        match py.detach(move || receiver.recv_timeout(Duration::from_millis(20))) {
            Ok(WorkerMessage::Log(line, acknowledge)) => {
                logs.push_str(&line);
                if interrupted.is_none()
                    && let Err(error) = relay_log(sys, &line)
                    && error.is_instance_of::<PyKeyboardInterrupt>(py)
                {
                    cancel.cancel();
                    interrupted = Some(error);
                }
                if acknowledge.is_some()
                    && let Err(error) = py.check_signals()
                {
                    cancel.cancel();
                    interrupted.get_or_insert(error);
                }
                // Closing the channel releases the worker even if log
                // delivery or signal handling raised a Python exception.
                drop(acknowledge);
            }
            Err(RecvTimeoutError::Timeout) if !worker.is_finished() => {}
            Ok(WorkerMessage::Finished) | Err(_) => break,
        }
    }
    if let Err(error) = py.check_signals() {
        interrupted.get_or_insert(error);
    }
    (logs, interrupted)
}

fn relay_log(sys: &Bound<'_, PyAny>, text: &str) -> PyResult<()> {
    let stderr = sys.getattr("stderr")?;
    if !stderr.is_none() {
        stderr.call_method1("write", (text,))?;
        stderr.call_method0("flush")?;
    }
    Ok(())
}
