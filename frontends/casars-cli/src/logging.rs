// SPDX-License-Identifier: LGPL-3.0-or-later
//! CLI logging flag/environment parsing, global subscriber initialization, and
//! front-end runner for `casars-cli` executables.

use std::ffi::OsString;
use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::{Arc, Mutex, PoisonError};

use casa_tables::log_table::{
    CasaLogLayer, CasaLogSink, CasaLoggingError, CasaPriority, CasaTableLogSink, CompositeSink,
    NullSink, TableLogOpenMode, WriterSink,
};
use casa_tasks::{EnterWorkspace, TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};
use thiserror::Error;
use tracing_subscriber::Registry;
use tracing_subscriber::layer::SubscriberExt;

/// CLI flag that enables CASA log-table output.
pub const LOG_TABLE_FLAG: &str = "--log-table";
/// CLI flag that controls the CASA log-table minimum priority.
pub const LOG_TABLE_PRIORITY_FLAG: &str = "--log-table-priority";
/// CLI flag that controls stderr logging, or disables it with `off`.
pub const LOG_STDERR_PRIORITY_FLAG: &str = "--log-stderr-priority";

/// Errors produced by CLI logging flag/environment parsing or subscriber setup.
#[derive(Debug, Error)]
pub enum CliLoggingError {
    /// Missing value for a CLI logging flag.
    #[error("missing value for {0}")]
    MissingOptionValue(&'static str),
    /// Non-UTF-8 value for a CLI logging flag.
    #[error("invalid non-UTF-8 value for {0}")]
    NonUtf8OptionValue(&'static str),
    /// Invalid priority string in `CASA_RS_LOG_*` environment variables.
    #[error("invalid {name} priority {value:?}")]
    InvalidEnvPriority {
        /// Environment variable name.
        name: &'static str,
        /// Invalid value text.
        value: String,
    },
    /// Invalid priority or sink setup error from `casa-logging`.
    #[error(transparent)]
    Logging(#[from] CasaLoggingError),
    /// Failed to install the global `tracing` subscriber.
    #[error("tracing subscriber setup failed: {0}")]
    Subscriber(String),
}

/// Application-level logging configuration for CLI binaries.
#[derive(Debug, Clone)]
pub struct CasaLoggingConfig {
    /// Optional path to a CASA LOG table.
    pub table_path: Option<PathBuf>,
    /// Minimum priority written to the CASA LOG table.
    pub table_priority: CasaPriority,
    /// Minimum priority written to stderr (`None` disables stderr output).
    pub stderr_priority: Option<CasaPriority>,
}

impl Default for CasaLoggingConfig {
    fn default() -> Self {
        Self::from_env().unwrap_or_else(|_| Self::fallback())
    }
}

impl CasaLoggingConfig {
    /// Whether the environment or command line explicitly selects an output sink.
    ///
    /// A priority for a log table alone does not select a table. An explicit
    /// stderr priority (including `off`) or a table path does select output.
    #[must_use]
    pub fn output_was_configured(args: &[OsString]) -> bool {
        output_was_configured(
            args,
            std::env::var_os("CASA_RS_LOG_TABLE").is_some(),
            std::env::var_os("CASA_RS_LOG_STDERR_PRIORITY").is_some(),
        )
    }

    /// Build logging configuration from `CASA_RS_LOG_*` environment variables.
    ///
    /// # Errors
    /// Returns [`CliLoggingError::InvalidEnvPriority`] if an environment
    /// variable holds an unknown priority name.
    pub fn from_env() -> Result<Self, CliLoggingError> {
        Ok(Self {
            table_path: std::env::var_os("CASA_RS_LOG_TABLE").map(PathBuf::from),
            table_priority: env_priority("CASA_RS_LOG_TABLE_PRIORITY")?
                .unwrap_or(CasaPriority::Info),
            stderr_priority: env_stderr_priority("CASA_RS_LOG_STDERR_PRIORITY")?,
        })
    }

    /// Apply the shared logging flags in `args` to this configuration and
    /// return the other arguments. Reads no environment: the caller chose the
    /// configuration the flags refine.
    ///
    /// # Errors
    /// Returns [`CliLoggingError`] if a logging flag lacks a value or specifies
    /// an invalid priority.
    pub fn with_cli_args(
        self,
        args: impl IntoIterator<Item = OsString>,
    ) -> Result<(Vec<OsString>, Self), CliLoggingError> {
        apply_cli_logging_args(args, self)
    }

    fn fallback() -> Self {
        Self {
            table_path: None,
            table_priority: CasaPriority::Info,
            stderr_priority: Some(CasaPriority::Warn),
        }
    }
}

fn output_was_configured(args: &[OsString], table: bool, stderr: bool) -> bool {
    table
        || stderr
        || args.iter().filter_map(|arg| arg.to_str()).any(|arg| {
            [LOG_TABLE_FLAG, LOG_STDERR_PRIORITY_FLAG]
                .iter()
                .any(|flag| {
                    arg == *flag
                        || arg
                            .strip_prefix(*flag)
                            .is_some_and(|rest| rest.starts_with('='))
                })
        })
}

fn env_priority(name: &'static str) -> Result<Option<CasaPriority>, CliLoggingError> {
    std::env::var(name)
        .ok()
        .map(|value| {
            CasaPriority::from_str(&value)
                .map_err(|_| CliLoggingError::InvalidEnvPriority { name, value })
        })
        .transpose()
}

fn env_stderr_priority(name: &'static str) -> Result<Option<CasaPriority>, CliLoggingError> {
    match std::env::var(name) {
        Ok(value) if priority_is_off(&value) => Ok(None),
        Ok(value) => CasaPriority::from_str(&value)
            .map(Some)
            .map_err(|_| CliLoggingError::InvalidEnvPriority { name, value }),
        Err(_) => Ok(Some(CasaPriority::Warn)),
    }
}

fn priority_is_off(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "off" | "none" | "quiet" | "false" | "0"
    )
}

/// Remove shared CASA logging flags without reading environment or initializing logging.
///
/// # Errors
/// Returns [`CliLoggingError`] if a logging flag lacks a value or specifies an
/// invalid priority.
pub fn strip_cli_logging_args(
    args: impl IntoIterator<Item = OsString>,
) -> Result<Vec<OsString>, CliLoggingError> {
    let (filtered, _) = apply_cli_logging_args(args, CasaLoggingConfig::fallback())?;
    Ok(filtered)
}

fn apply_cli_logging_args(
    args: impl IntoIterator<Item = OsString>,
    mut config: CasaLoggingConfig,
) -> Result<(Vec<OsString>, CasaLoggingConfig), CliLoggingError> {
    let mut filtered = Vec::new();
    let mut args = args.into_iter().peekable();
    while let Some(arg) = args.next() {
        if let Some(value) = split_option_value(&arg, LOG_TABLE_FLAG) {
            config.table_path = Some(PathBuf::from(value));
            continue;
        }
        if arg == LOG_TABLE_FLAG {
            let value = next_option_value(&mut args, LOG_TABLE_FLAG)?;
            config.table_path = Some(PathBuf::from(value));
            continue;
        }
        if let Some(value) = split_option_value(&arg, LOG_TABLE_PRIORITY_FLAG) {
            config.table_priority = CasaPriority::from_str(&value)?;
            continue;
        }
        if arg == LOG_TABLE_PRIORITY_FLAG {
            let value = next_option_value(&mut args, LOG_TABLE_PRIORITY_FLAG)?;
            config.table_priority = parse_priority_option(&value, LOG_TABLE_PRIORITY_FLAG)?;
            continue;
        }
        if let Some(value) = split_option_value(&arg, LOG_STDERR_PRIORITY_FLAG) {
            config.stderr_priority = parse_optional_priority_str(&value)?;
            continue;
        }
        if arg == LOG_STDERR_PRIORITY_FLAG {
            let value = next_option_value(&mut args, LOG_STDERR_PRIORITY_FLAG)?;
            config.stderr_priority = parse_optional_priority(&value)?;
            continue;
        }
        filtered.push(arg);
    }
    Ok((filtered, config))
}

fn split_option_value(arg: &OsString, flag: &'static str) -> Option<String> {
    let arg = arg.to_str()?;
    let rest = arg.strip_prefix(flag)?;
    rest.strip_prefix('=').map(str::to_string)
}

fn next_option_value(
    args: &mut impl Iterator<Item = OsString>,
    flag: &'static str,
) -> Result<OsString, CliLoggingError> {
    args.next().ok_or(CliLoggingError::MissingOptionValue(flag))
}

fn parse_optional_priority(value: &OsString) -> Result<Option<CasaPriority>, CliLoggingError> {
    let value = value.to_str().ok_or(CliLoggingError::NonUtf8OptionValue(
        LOG_STDERR_PRIORITY_FLAG,
    ))?;
    parse_optional_priority_str(value)
}

fn parse_optional_priority_str(value: &str) -> Result<Option<CasaPriority>, CliLoggingError> {
    if priority_is_off(value) {
        Ok(None)
    } else {
        Ok(Some(CasaPriority::from_str(value)?))
    }
}

fn parse_priority_option(
    value: &OsString,
    flag: &'static str,
) -> Result<CasaPriority, CliLoggingError> {
    let value = value
        .to_str()
        .ok_or(CliLoggingError::NonUtf8OptionValue(flag))?;
    Ok(CasaPriority::from_str(value)?)
}

/// Guard holding sink state for explicit flush at task exit.
#[derive(Clone)]
pub struct CasaLoggingGuard {
    sink: Arc<dyn CasaLogSink>,
}

impl CasaLoggingGuard {
    /// Flush the configured logging sinks.
    ///
    /// # Errors
    /// Returns [`CasaLoggingError`] if flushing any sink fails.
    pub fn flush(&self) -> Result<(), CasaLoggingError> {
        self.sink.flush()
    }
}

impl Drop for CasaLoggingGuard {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

/// Initialize global logging once with stderr records written to `stderr`.
///
/// # Errors
/// Returns [`CliLoggingError`] if installing the global `tracing` subscriber fails.
pub fn init_global_with_stderr<W: Write + Send + 'static>(
    config: CasaLoggingConfig,
    stderr: Arc<Mutex<W>>,
) -> Result<CasaLoggingGuard, CliLoggingError> {
    let mut sinks: Vec<Arc<dyn CasaLogSink>> = Vec::new();
    if let Some(path) = config.table_path {
        sinks.push(Arc::new(CasaTableLogSink::new(
            path,
            config.table_priority,
            TableLogOpenMode::CreateOrAppend,
        )));
    }
    if let Some(priority) = config.stderr_priority {
        sinks.push(Arc::new(WriterSink::new(stderr, priority)));
    }
    let sink: Arc<dyn CasaLogSink> = if sinks.is_empty() {
        Arc::new(NullSink)
    } else {
        Arc::new(CompositeSink::new(sinks))
    };
    tracing::subscriber::set_global_default(
        Registry::default().with(CasaLogLayer::filtered(sink.clone())),
    )
    .map_err(|error| CliLoggingError::Subscriber(error.to_string()))?;
    Ok(CasaLoggingGuard { sink })
}

/// Run a [`TaskFrontEnd`] as a binary `main`: strip CLI logging flags,
/// install process-global logging only for an executing action (`Run` / `JsonRun`),
/// enter the workspace when running on the command line, and write stdout/stderr.
pub fn run_front_end<F, W: Write + Send + 'static>(
    front_end: &TaskFrontEnd<F>,
    args: impl IntoIterator<Item = OsString>,
    logging: Result<CasaLoggingConfig, CliLoggingError>,
    enter_workspace: EnterWorkspace,
    stdout: &mut dyn Write,
    stderr: W,
) -> ExitCode
where
    F: Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>,
{
    let raw_args: Vec<OsString> = args.into_iter().collect();
    let default_stderr_logs = !CasaLoggingConfig::output_was_configured(&raw_args);
    let stderr = Arc::new(Mutex::new(stderr));
    let mut guard: Option<CasaLoggingGuard> = None;

    let output = match strip_cli_logging_args(raw_args.clone()) {
        Err(error) => casa_tasks::FrontEndOutput {
            stdout: String::new(),
            stderr: vec![format!("Error: {error}")],
            code: casa_contracts::EXIT_VALIDATION_FAILED,
        },
        Ok(stripped) => {
            front_end.dispatch_for_cli(&stripped, enter_workspace, default_stderr_logs, || {
                let (_, mut config) = logging?.with_cli_args(raw_args)?;
                // Named from where the command was run, before a run enters its workspace.
                config.table_path = config
                    .table_path
                    .map(|path| std::path::absolute(&path).unwrap_or(path));
                guard = Some(init_global_with_stderr(config, stderr.clone())?);
                Ok::<_, CliLoggingError>(())
            })
        }
    };

    let _ = stdout.write_all(output.stdout.as_bytes());
    let _ = stdout.flush();
    let write_lines = |lines: &[String]| {
        let mut stderr = stderr.lock().unwrap_or_else(PoisonError::into_inner);
        for line in lines {
            let _ = writeln!(stderr, "{line}");
        }
        let _ = stderr.flush();
    };
    write_lines(&output.stderr);
    if let Some(Err(error)) = guard.as_ref().map(CasaLoggingGuard::flush) {
        write_lines(&[format!("Error: failed to flush logging: {error}")]);
    }
    ExitCode::from(u8::try_from(output.code).unwrap_or(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_contracts::{
        CasaValue, ProviderCliMachineActions, ProviderCliProjection, ProviderProjectionMetadata,
        ProviderProtocolDescriptor, ProviderSurfaceKind, TaskFailureKind, TaskOperationDescriptor,
        TaskProviderContract, TaskProviderSchemas, TaskReply, TaskSemanticContract,
        builtin_surface_bundle, merged_components,
    };
    use schemars::schema_for;
    use std::cell::RefCell;
    use std::path::PathBuf;
    use std::sync::{Mutex, OnceLock};

    fn global_state_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn clear_log_env() {
        unsafe {
            std::env::remove_var("CASA_RS_LOG_TABLE");
            std::env::remove_var("CASA_RS_LOG_TABLE_PRIORITY");
            std::env::remove_var("CASA_RS_LOG_STDERR_PRIORITY");
        }
    }

    #[test]
    fn cli_flags_refine_a_chosen_configuration_without_the_environment() {
        let _lock = global_state_lock();
        clear_log_env();
        unsafe {
            std::env::set_var("CASA_RS_LOG_TABLE_PRIORITY", "INVALID");
            std::env::set_var("CASA_RS_LOG_TABLE", "/from/the/environment");
        }
        let chosen = CasaLoggingConfig {
            table_path: None,
            table_priority: CasaPriority::Info,
            stderr_priority: Some(CasaPriority::Warn),
        };
        let (rest, config) = chosen
            .with_cli_args(
                ["--log-stderr-priority", "off", "x.image"]
                    .into_iter()
                    .map(OsString::from),
            )
            .unwrap();
        clear_log_env();
        assert_eq!(rest, [OsString::from("x.image")]);
        assert_eq!(config.table_path, None);
        assert_eq!(config.table_priority, CasaPriority::Info);
        assert_eq!(config.stderr_priority, None);
    }

    #[test]
    fn cli_logging_args_are_stripped_and_applied() {
        let _guard = global_state_lock();
        clear_log_env();
        let config = CasaLoggingConfig::from_env().expect("env config");
        let (filtered, config) = config
            .with_cli_args([
                OsString::from("--vis"),
                OsString::from("input.ms"),
                OsString::from("--log-table"),
                OsString::from("run.log"),
                OsString::from("--log-table-priority=DEBUG1"),
                OsString::from("--log-stderr-priority"),
                OsString::from("off"),
            ])
            .expect("parse logging args");
        assert_eq!(
            filtered,
            vec![OsString::from("--vis"), OsString::from("input.ms")]
        );
        assert_eq!(config.table_path, Some(PathBuf::from("run.log")));
        assert_eq!(config.table_priority, CasaPriority::Debug1);
        assert_eq!(config.stderr_priority, None);
        clear_log_env();
    }

    #[test]
    fn cli_logging_args_can_be_stripped_without_env_config() {
        let _guard = global_state_lock();
        clear_log_env();
        unsafe {
            std::env::set_var("CASA_RS_LOG_TABLE_PRIORITY", "invalid");
        }
        let filtered = strip_cli_logging_args([
            OsString::from("--log-table"),
            OsString::from("run.log"),
            OsString::from("--log-table-priority"),
            OsString::from("DEBUG1"),
            OsString::from("--log-stderr-priority=off"),
            OsString::from("--vis"),
            OsString::from("input.ms"),
        ])
        .expect("strip logging args without reading env");
        assert_eq!(
            filtered,
            vec![OsString::from("--vis"), OsString::from("input.ms")]
        );
        clear_log_env();
    }

    #[test]
    fn env_logging_config_supports_stderr_off() {
        let _guard = global_state_lock();
        clear_log_env();
        unsafe {
            std::env::set_var("CASA_RS_LOG_TABLE", "env.log");
            std::env::set_var("CASA_RS_LOG_TABLE_PRIORITY", "WARN");
            std::env::set_var("CASA_RS_LOG_STDERR_PRIORITY", "off");
        }
        let config = CasaLoggingConfig::from_env().expect("env logging config");
        assert_eq!(config.table_path, Some(PathBuf::from("env.log")));
        assert_eq!(config.table_priority, CasaPriority::Warn);
        assert_eq!(config.stderr_priority, None);
        clear_log_env();
    }

    #[test]
    fn invalid_env_priority_is_reported() {
        let _guard = global_state_lock();
        clear_log_env();
        unsafe {
            std::env::set_var("CASA_RS_LOG_TABLE_PRIORITY", "loud");
        }
        assert!(matches!(
            CasaLoggingConfig::from_env(),
            Err(CliLoggingError::InvalidEnvPriority {
                name: "CASA_RS_LOG_TABLE_PRIORITY",
                ..
            })
        ));
        clear_log_env();
    }

    #[test]
    fn selecting_a_sink_is_distinct_from_setting_only_a_table_priority() {
        assert!(!output_was_configured(
            &["--log-table-priority".into(), "WARN".into()],
            false,
            false
        ));
        assert!(output_was_configured(
            &["--log-stderr-priority=off".into()],
            false,
            false
        ));
        assert!(output_was_configured(
            &["--log-table".into(), "history.log".into()],
            false,
            false
        ));
        assert!(output_was_configured(&[], true, false));
        assert!(output_was_configured(&[], false, true));
    }

    #[derive(Clone, Default)]
    struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

    impl SharedBuffer {
        fn text(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    impl Write for SharedBuffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[derive(serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
    struct Request {
        imagename: Vec<String>,
    }

    type Calls = RefCell<Vec<String>>;

    fn test_contract() -> TaskProviderContract {
        let request = schema_for!(Request);
        let result = schema_for!(());
        TaskProviderContract {
            protocol: ProviderProtocolDescriptor::new(
                "casa_imexplore_task",
                1,
                ProviderSurfaceKind::Task,
                "0.27.0",
            ),
            semantic: TaskSemanticContract {
                request_schema: request.clone(),
                result_schema: result.clone(),
                operations: vec![TaskOperationDescriptor {
                    name: "run".into(),
                    request_kind: "run".into(),
                    result_kind: Some("run".into()),
                }],
                front_end: None,
            },
            components: merged_components([&request, &result]),
            annotations: serde_json::json!({}),
            projections: ProviderProjectionMetadata {
                cli: Some(ProviderCliProjection {
                    machine_actions: ProviderCliMachineActions {
                        json_schema: Some("--json-schema".into()),
                        protocol_info: Some("--protocol-info".into()),
                        json_run: Some("--json-run <SOURCE>".into()),
                    },
                }),
                python: None,
            },
            parameter_surfaces: vec![
                builtin_surface_bundle("imstat").unwrap(),
                builtin_surface_bundle("imhead").unwrap(),
            ],
            domain_schemas: TaskProviderSchemas {
                request_schema: request,
                result_schema: result,
            },
        }
    }

    fn test_front_end<'a>(
        calls: &'a Calls,
    ) -> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> + 'a> {
        TaskFrontEnd::new(
            "imexplore",
            test_contract(),
            &["imstat", "imhead"],
            move |run| {
                let imagename = run.plain_values()["imagename"]
                    .as_array()
                    .and_then(|values| values.first())
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                if imagename == "fail.image" {
                    return Err(TaskFailure::execution("the image is unreadable"));
                }
                calls
                    .borrow_mut()
                    .push(format!("{}:{imagename}", run.surface()));
                TaskSuccess::new(CasaValue::None, &())
            },
        )
        .unwrap()
    }

    fn args(words: &[&str]) -> Vec<OsString> {
        words.iter().map(OsString::from).collect()
    }

    fn in_workspace(workspace: &std::path::Path, extra: &[&str]) -> Vec<OsString> {
        let mut out = vec![
            OsString::from("--workspace"),
            workspace.as_os_str().to_os_string(),
        ];
        out.extend(args(extra));
        out
    }

    fn quiet_logging() -> Result<CasaLoggingConfig, CliLoggingError> {
        Ok(CasaLoggingConfig {
            table_path: None,
            table_priority: CasaPriority::Info,
            stderr_priority: Some(CasaPriority::Info),
        })
    }

    fn bad_logging() -> Result<CasaLoggingConfig, CliLoggingError> {
        Err(CliLoggingError::InvalidEnvPriority {
            name: "CASA_RS_LOG_TABLE_PRIORITY",
            value: "INVALID".into(),
        })
    }

    fn stay(_: PathBuf) -> std::io::Result<()> {
        Ok(())
    }

    #[test]
    fn main_writes_only_to_the_streams_its_binary_passes() {
        let calls = Calls::default();
        let front_end = test_front_end(&calls);
        let (mut stdout, stderr) = (Vec::new(), SharedBuffer::default());
        let code = run_front_end(
            &front_end,
            args(&["--template", "x.image"]),
            quiet_logging(),
            stay,
            &mut stdout,
            stderr.clone(),
        );
        assert_eq!(code, ExitCode::from(2));
        assert!(stdout.is_empty());
        assert!(
            stderr
                .text()
                .contains("--template takes no other arguments")
        );
    }

    #[test]
    fn a_bad_logging_configuration_refuses_only_a_run() {
        let calls = Calls::default();
        let front_end = test_front_end(&calls);
        let workspace = tempfile::tempdir().unwrap();
        let run = |line: Vec<OsString>| {
            let (mut stdout, stderr) = (Vec::new(), SharedBuffer::default());
            let code = run_front_end(
                &front_end,
                line,
                bad_logging(),
                stay,
                &mut stdout,
                stderr.clone(),
            );
            (code, String::from_utf8(stdout).unwrap(), stderr.text())
        };

        for line in [
            args(&["--template"]),
            args(&["--describe"]),
            args(&["--json-schema"]),
            args(&["--log-stderr-priority", "INFO", "--show", "x.image"]),
        ] {
            let (code, stdout, stderr) = run(line.clone());
            assert_eq!(code, ExitCode::SUCCESS, "{line:?}: {stderr}");
            assert!(!stdout.is_empty(), "{line:?}");
        }

        let (code, stdout, stderr) = run(in_workspace(workspace.path(), &["x.image"]));
        assert_eq!(code, ExitCode::from(2));
        assert!(stdout.is_empty());
        assert!(
            stderr.contains("failed to initialize logging: invalid CASA_RS_LOG_TABLE_PRIORITY"),
            "{stderr}"
        );

        let request = workspace.path().join("request.json");
        std::fs::write(
            &request,
            r#"{"surface":"imstat","values":{"imagename":"x.image"}}"#,
        )
        .unwrap();
        let (code, stdout, _) = run(args(&["--json-run", request.to_str().unwrap()]));
        assert_eq!(code, ExitCode::from(2));
        let reply: TaskReply = serde_json::from_str(&stdout).unwrap();
        assert!(
            matches!(&reply, TaskReply::Failed { kind: TaskFailureKind::Validation, message, .. }
                if message.contains("failed to initialize logging")),
            "{reply:?}"
        );

        // A malformed logging flag is refused on every action.
        let (code, _, stderr) = run(args(&["--template", "--log-table"]));
        assert_eq!(code, ExitCode::from(2));
        assert!(stderr.contains("missing value for --log-table"), "{stderr}");

        assert!(calls.borrow().is_empty());
    }

    const LOGGING_CHILD: &str = "CASA_RS_FRONT_END_LOGGING_CHILD";

    #[test]
    fn a_failed_run_logged_in_a_child_process() {
        let Some(report) = std::env::var_os(LOGGING_CHILD) else {
            return;
        };
        let calls = Calls::default();
        let front_end = test_front_end(&calls);
        let workspace = tempfile::tempdir().unwrap();
        let (mut stdout, stderr) = (Vec::new(), SharedBuffer::default());
        let code = run_front_end(
            &front_end,
            in_workspace(workspace.path(), &["fail.image", "--no-save-last"]),
            quiet_logging(),
            stay,
            &mut stdout,
            stderr.clone(),
        );
        assert_eq!(code, ExitCode::from(1));
        std::fs::write(report, stderr.text()).unwrap();
    }

    #[test]
    fn a_runs_log_reaches_only_the_stderr_its_binary_passes() {
        if std::env::var_os(LOGGING_CHILD).is_some() {
            return;
        }
        let directory = tempfile::tempdir().unwrap();
        let report = directory.path().join("stderr.txt");
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "logging::tests::a_failed_run_logged_in_a_child_process",
                "--nocapture",
                "--test-threads",
                "1",
            ])
            .env(LOGGING_CHILD, &report)
            .output()
            .unwrap();
        let process_stderr = String::from_utf8_lossy(&child.stderr);
        assert!(child.status.success(), "{process_stderr}");
        let supplied = std::fs::read_to_string(&report).expect("the child ran");
        assert!(supplied.contains("SEVERE"), "{supplied}");
        assert!(supplied.contains("imstat failed"), "{supplied}");
        assert!(supplied.contains("INFO"), "{supplied}");
        assert!(
            supplied.contains("Error: the image is unreadable"),
            "{supplied}"
        );
        assert!(!process_stderr.contains("SEVERE"), "{process_stderr}");
        assert!(!process_stderr.contains("imstat"), "{process_stderr}");
    }
}
