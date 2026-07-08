// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA-compatible logging sinks and `tracing` integration.
//!
//! The instrumentation facade for workspace code is [`tracing`]. This crate
//! owns only the CASA-specific priority model, log-table serialization, sink
//! fan-out, and application initialization helpers.

use std::ffi::OsString;
use std::fmt::{self, Write as _};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use casa_tables::{ColumnSchema, ColumnType, DataManagerKind, Table, TableInfo, TableOptions};
use casa_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
use thiserror::Error;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{Layer, Registry};

const UNIX_EPOCH_MJD_DAYS: f64 = 40587.0;
const SECONDS_PER_DAY: f64 = 86400.0;
const LOG_TABLE_README: &str = "Repository for software-generated logging messages";
const TIME_COLUMN_COMMENT: &str = "MJD in seconds";
/// CLI flag that enables CASA log-table output.
pub const LOG_TABLE_FLAG: &str = "--log-table";
/// CLI flag that controls the CASA log-table minimum priority.
pub const LOG_TABLE_PRIORITY_FLAG: &str = "--log-table-priority";
/// CLI flag that controls stderr logging, or disables it with `off`.
pub const LOG_STDERR_PRIORITY_FLAG: &str = "--log-stderr-priority";

/// CASA/casacore log-message priority, in ascending severity order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CasaPriority {
    Debugging,
    Debug2,
    Debug1,
    Info5,
    Info4,
    Info3,
    Info2,
    Info1,
    Info,
    Warn,
    Severe,
}

impl CasaPriority {
    /// All CASA priorities in ascending severity order.
    pub const ALL: [Self; 11] = [
        Self::Debugging,
        Self::Debug2,
        Self::Debug1,
        Self::Info5,
        Self::Info4,
        Self::Info3,
        Self::Info2,
        Self::Info1,
        Self::Info,
        Self::Warn,
        Self::Severe,
    ];

    /// Return the exact string persisted by casacore `LogMessage::toString`.
    pub fn as_casa_str(self) -> &'static str {
        match self {
            Self::Debugging => "DEBUGGING",
            Self::Debug2 => "DEBUG2",
            Self::Debug1 => "DEBUG1",
            Self::Info5 => "INFO5",
            Self::Info4 => "INFO4",
            Self::Info3 => "INFO3",
            Self::Info2 => "INFO2",
            Self::Info1 => "INFO1",
            Self::Info => "INFO",
            Self::Warn => "WARN",
            Self::Severe => "SEVERE",
        }
    }

    /// Map a `tracing` level to the default CASA priority.
    pub fn from_tracing_level(level: &Level) -> Self {
        match *level {
            Level::ERROR => Self::Severe,
            Level::WARN => Self::Warn,
            Level::INFO => Self::Info,
            Level::DEBUG => Self::Debug1,
            Level::TRACE => Self::Debugging,
        }
    }
}

impl fmt::Display for CasaPriority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_casa_str())
    }
}

impl FromStr for CasaPriority {
    type Err = CasaLoggingError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_uppercase().as_str() {
            "DEBUGGING" | "TRACE" => Ok(Self::Debugging),
            "DEBUG2" => Ok(Self::Debug2),
            "DEBUG1" | "DEBUG" => Ok(Self::Debug1),
            "INFO5" => Ok(Self::Info5),
            "INFO4" => Ok(Self::Info4),
            "INFO3" => Ok(Self::Info3),
            "INFO2" => Ok(Self::Info2),
            "INFO1" => Ok(Self::Info1),
            "INFO" | "NORMAL" => Ok(Self::Info),
            "WARN" | "WARNING" => Ok(Self::Warn),
            "SEVERE" | "ERROR" => Ok(Self::Severe),
            other => Err(CasaLoggingError::InvalidPriority(other.to_string())),
        }
    }
}

/// A CASA log record ready for sink delivery.
#[derive(Debug, Clone, PartialEq)]
pub struct CasaLogRecord {
    pub time_mjd_seconds: f64,
    pub priority: CasaPriority,
    pub message: String,
    pub location: String,
    pub object_id: String,
}

impl CasaLogRecord {
    /// Create a record with the current wall-clock timestamp.
    pub fn new(
        priority: CasaPriority,
        message: impl Into<String>,
        location: impl Into<String>,
    ) -> Self {
        Self {
            time_mjd_seconds: system_time_to_mjd_seconds(SystemTime::now()),
            priority,
            message: trim_message(message.into()),
            location: location.into(),
            object_id: String::new(),
        }
    }

    /// Attach a CASA object id string.
    pub fn with_object_id(mut self, object_id: impl Into<String>) -> Self {
        self.object_id = object_id.into();
        self
    }
}

/// Convert `SystemTime` to CASA TableLogSink TIME units: MJD seconds UTC.
pub fn system_time_to_mjd_seconds(time: SystemTime) -> f64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration_to_unix_seconds(duration) + UNIX_EPOCH_MJD_DAYS * SECONDS_PER_DAY,
        Err(error) => {
            UNIX_EPOCH_MJD_DAYS * SECONDS_PER_DAY - duration_to_unix_seconds(error.duration())
        }
    }
}

fn duration_to_unix_seconds(duration: Duration) -> f64 {
    duration.as_secs() as f64 + f64::from(duration.subsec_nanos()) / 1_000_000_000.0
}

fn trim_message(mut message: String) -> String {
    while message.ends_with('\n') || message.ends_with('\r') {
        message.pop();
    }
    message
}

/// Errors produced by CASA logging setup, sinks, and table serialization.
#[derive(Debug, Error)]
pub enum CasaLoggingError {
    #[error("invalid CASA log priority {0:?}")]
    InvalidPriority(String),
    #[error("missing value for {0}")]
    MissingOptionValue(&'static str),
    #[error("invalid non-UTF-8 value for {0}")]
    NonUtf8OptionValue(&'static str),
    #[error("invalid {name} priority {value:?}")]
    InvalidEnvPriority { name: &'static str, value: String },
    #[error("log table schema is missing required column {0}")]
    MissingColumn(&'static str),
    #[error("log table has incompatible TableInfo type {0:?}")]
    IncompatibleTableInfo(String),
    #[error("log table column {column} has incompatible type")]
    IncompatibleColumn { column: &'static str },
    #[error("sink lock was poisoned")]
    PoisonedLock,
    #[error("table error: {0}")]
    Table(#[from] casa_tables::TableError),
    #[error("tracing subscriber setup failed: {0}")]
    Subscriber(String),
}

/// Runtime sink for CASA log records.
pub trait CasaLogSink: Send + Sync {
    fn enabled(&self, priority: CasaPriority) -> bool;
    fn write(&self, record: &CasaLogRecord) -> Result<(), CasaLoggingError>;
    fn flush(&self) -> Result<(), CasaLoggingError>;
}

/// Sink that discards every record.
#[derive(Debug, Default)]
pub struct NullSink;

impl CasaLogSink for NullSink {
    fn enabled(&self, _priority: CasaPriority) -> bool {
        false
    }

    fn write(&self, _record: &CasaLogRecord) -> Result<(), CasaLoggingError> {
        Ok(())
    }

    fn flush(&self) -> Result<(), CasaLoggingError> {
        Ok(())
    }
}

/// In-memory sink for tests and embedded callers.
#[derive(Debug)]
pub struct MemorySink {
    threshold: CasaPriority,
    records: Mutex<Vec<CasaLogRecord>>,
}

impl MemorySink {
    pub fn new(threshold: CasaPriority) -> Self {
        Self {
            threshold,
            records: Mutex::new(Vec::new()),
        }
    }

    pub fn records(&self) -> Result<Vec<CasaLogRecord>, CasaLoggingError> {
        Ok(self
            .records
            .lock()
            .map_err(|_| CasaLoggingError::PoisonedLock)?
            .clone())
    }
}

impl CasaLogSink for MemorySink {
    fn enabled(&self, priority: CasaPriority) -> bool {
        priority >= self.threshold
    }

    fn write(&self, record: &CasaLogRecord) -> Result<(), CasaLoggingError> {
        if self.enabled(record.priority) {
            self.records
                .lock()
                .map_err(|_| CasaLoggingError::PoisonedLock)?
                .push(record.clone());
        }
        Ok(())
    }

    fn flush(&self) -> Result<(), CasaLoggingError> {
        Ok(())
    }
}

/// Stderr sink with CASA-like text formatting.
#[derive(Debug)]
pub struct StderrSink {
    threshold: CasaPriority,
}

impl StderrSink {
    pub fn new(threshold: CasaPriority) -> Self {
        Self { threshold }
    }
}

impl CasaLogSink for StderrSink {
    fn enabled(&self, priority: CasaPriority) -> bool {
        priority >= self.threshold
    }

    fn write(&self, record: &CasaLogRecord) -> Result<(), CasaLoggingError> {
        if self.enabled(record.priority) {
            eprintln!(
                "{} {} {}",
                record.priority.as_casa_str(),
                record.location,
                record.message
            );
        }
        Ok(())
    }

    fn flush(&self) -> Result<(), CasaLoggingError> {
        Ok(())
    }
}

/// Fan-out sink that writes each record to all child sinks.
#[derive(Default)]
pub struct CompositeSink {
    sinks: Vec<Arc<dyn CasaLogSink>>,
}

impl CompositeSink {
    pub fn new(sinks: Vec<Arc<dyn CasaLogSink>>) -> Self {
        Self { sinks }
    }
}

impl CasaLogSink for CompositeSink {
    fn enabled(&self, priority: CasaPriority) -> bool {
        self.sinks.iter().any(|sink| sink.enabled(priority))
    }

    fn write(&self, record: &CasaLogRecord) -> Result<(), CasaLoggingError> {
        for sink in &self.sinks {
            if sink.enabled(record.priority) {
                sink.write(record)?;
            }
        }
        Ok(())
    }

    fn flush(&self) -> Result<(), CasaLoggingError> {
        for sink in &self.sinks {
            sink.flush()?;
        }
        Ok(())
    }
}

/// Open mode for CASA log-table sinks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableLogOpenMode {
    CreateOrAppend,
    CreateNew,
    AppendExisting,
}

/// Buffered sink that writes casacore `TableLogSink` compatible rows.
#[derive(Debug)]
pub struct CasaTableLogSink {
    path: PathBuf,
    threshold: CasaPriority,
    mode: TableLogOpenMode,
    pending: Mutex<Vec<CasaLogRecord>>,
}

impl CasaTableLogSink {
    pub fn new(path: impl Into<PathBuf>, threshold: CasaPriority, mode: TableLogOpenMode) -> Self {
        Self {
            path: path.into(),
            threshold,
            mode,
            pending: Mutex::new(Vec::new()),
        }
    }

    pub fn pending_len(&self) -> Result<usize, CasaLoggingError> {
        Ok(self
            .pending
            .lock()
            .map_err(|_| CasaLoggingError::PoisonedLock)?
            .len())
    }

    fn flush_records(&self, records: &[CasaLogRecord]) -> Result<(), CasaLoggingError> {
        if records.is_empty() {
            return Ok(());
        }
        let mut table = open_or_create_log_table(&self.path, self.mode)?;
        validate_log_table(&table)?;
        for record in records {
            table.add_row(log_record_to_row(record))?;
        }
        table.save(log_table_options(&self.path))?;
        Ok(())
    }
}

impl CasaLogSink for CasaTableLogSink {
    fn enabled(&self, priority: CasaPriority) -> bool {
        priority >= self.threshold
    }

    fn write(&self, record: &CasaLogRecord) -> Result<(), CasaLoggingError> {
        if !self.enabled(record.priority) {
            return Ok(());
        }
        {
            self.pending
                .lock()
                .map_err(|_| CasaLoggingError::PoisonedLock)?
                .push(record.clone());
        }
        if record.priority >= CasaPriority::Warn {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&self) -> Result<(), CasaLoggingError> {
        let records = {
            let mut pending = self
                .pending
                .lock()
                .map_err(|_| CasaLoggingError::PoisonedLock)?;
            if pending.is_empty() {
                return Ok(());
            }
            std::mem::take(&mut *pending)
        };
        if let Err(error) = self.flush_records(&records) {
            let mut pending = self
                .pending
                .lock()
                .map_err(|_| CasaLoggingError::PoisonedLock)?;
            let mut retry = records;
            retry.append(&mut *pending);
            *pending = retry;
            return Err(error);
        }
        Ok(())
    }
}

/// Create the canonical CASA log table schema.
pub fn log_table_schema() -> casa_tables::TableSchema {
    casa_tables::TableSchema::new(vec![
        ColumnSchema::scalar("TIME", PrimitiveType::Float64).with_comment(TIME_COLUMN_COMMENT),
        ColumnSchema::scalar("PRIORITY", PrimitiveType::String).with_max_length(9),
        ColumnSchema::scalar("MESSAGE", PrimitiveType::String),
        ColumnSchema::scalar("LOCATION", PrimitiveType::String),
        ColumnSchema::scalar("OBJECT_ID", PrimitiveType::String),
    ])
    .expect("canonical log table schema is valid")
}

/// Build a new empty CASA-compatible log table.
pub fn create_log_table() -> Table {
    let mut table = Table::with_schema(log_table_schema());
    table.set_info(TableInfo {
        table_type: "LOG".to_string(),
        sub_type: String::new(),
        readme: vec![LOG_TABLE_README.to_string()],
    });
    table.set_column_keywords("TIME", time_column_keywords());
    table
}

fn time_column_keywords() -> RecordValue {
    RecordValue::new(vec![
        RecordField::new("UNIT", Value::Scalar(ScalarValue::String("s".to_string()))),
        RecordField::new(
            "MEASURE_TYPE",
            Value::Scalar(ScalarValue::String("EPOCH".to_string())),
        ),
        RecordField::new(
            "MEASURE_REFERENCE",
            Value::Scalar(ScalarValue::String("UTC".to_string())),
        ),
    ])
}

fn open_or_create_log_table(
    path: &Path,
    mode: TableLogOpenMode,
) -> Result<Table, CasaLoggingError> {
    match mode {
        TableLogOpenMode::CreateNew => Ok(create_log_table()),
        TableLogOpenMode::AppendExisting => {
            Table::open(TableOptions::new(path)).map_err(Into::into)
        }
        TableLogOpenMode::CreateOrAppend => {
            if path.exists() {
                Table::open(TableOptions::new(path)).map_err(Into::into)
            } else {
                Ok(create_log_table())
            }
        }
    }
}

fn log_table_options(path: &Path) -> TableOptions {
    TableOptions::new(path)
        .with_data_manager(DataManagerKind::StandardStMan)
        .with_data_manager_group("SSM")
}

/// Validate that a table can receive canonical CASA log rows.
pub fn validate_log_table(table: &Table) -> Result<(), CasaLoggingError> {
    let table_type = table.info().table_type.as_str();
    if !matches!(table_type, "LOG" | "Log message") {
        return Err(CasaLoggingError::IncompatibleTableInfo(
            table.info().table_type.clone(),
        ));
    }
    let schema = table
        .schema()
        .ok_or(CasaLoggingError::MissingColumn("TIME"))?;
    for (name, primitive) in [
        ("TIME", PrimitiveType::Float64),
        ("PRIORITY", PrimitiveType::String),
        ("MESSAGE", PrimitiveType::String),
        ("LOCATION", PrimitiveType::String),
        ("OBJECT_ID", PrimitiveType::String),
    ] {
        let column = schema
            .column(name)
            .ok_or(CasaLoggingError::MissingColumn(name))?;
        if !matches!(column.column_type(), ColumnType::Scalar)
            || column.data_type() != Some(primitive)
        {
            return Err(CasaLoggingError::IncompatibleColumn { column: name });
        }
    }
    Ok(())
}

fn log_record_to_row(record: &CasaLogRecord) -> RecordValue {
    RecordValue::new(vec![
        RecordField::new(
            "TIME",
            Value::Scalar(ScalarValue::Float64(record.time_mjd_seconds)),
        ),
        RecordField::new(
            "PRIORITY",
            Value::Scalar(ScalarValue::String(
                record.priority.as_casa_str().to_string(),
            )),
        ),
        RecordField::new(
            "MESSAGE",
            Value::Scalar(ScalarValue::String(record.message.clone())),
        ),
        RecordField::new(
            "LOCATION",
            Value::Scalar(ScalarValue::String(record.location.clone())),
        ),
        RecordField::new(
            "OBJECT_ID",
            Value::Scalar(ScalarValue::String(record.object_id.clone())),
        ),
    ])
}

/// Application-level logging configuration.
#[derive(Debug, Clone)]
pub struct CasaLoggingConfig {
    pub table_path: Option<PathBuf>,
    pub table_priority: CasaPriority,
    pub stderr_priority: Option<CasaPriority>,
}

impl Default for CasaLoggingConfig {
    fn default() -> Self {
        Self::from_env().unwrap_or_else(|_| Self::fallback())
    }
}

impl CasaLoggingConfig {
    /// Build logging configuration from `CASA_RS_LOG_*` environment variables.
    pub fn from_env() -> Result<Self, CasaLoggingError> {
        Ok(Self {
            table_path: std::env::var_os("CASA_RS_LOG_TABLE").map(PathBuf::from),
            table_priority: env_priority("CASA_RS_LOG_TABLE_PRIORITY")?
                .unwrap_or(CasaPriority::Info),
            stderr_priority: env_stderr_priority("CASA_RS_LOG_STDERR_PRIORITY")?,
        })
    }

    fn fallback() -> Self {
        Self {
            table_path: None,
            table_priority: CasaPriority::Info,
            stderr_priority: Some(CasaPriority::Warn),
        }
    }
}

fn env_priority(name: &'static str) -> Result<Option<CasaPriority>, CasaLoggingError> {
    std::env::var(name)
        .ok()
        .map(|value| {
            CasaPriority::from_str(&value)
                .map_err(|_| CasaLoggingError::InvalidEnvPriority { name, value })
        })
        .transpose()
}

fn env_stderr_priority(name: &'static str) -> Result<Option<CasaPriority>, CasaLoggingError> {
    match std::env::var(name) {
        Ok(value) if priority_is_off(&value) => Ok(None),
        Ok(value) => CasaPriority::from_str(&value)
            .map(Some)
            .map_err(|_| CasaLoggingError::InvalidEnvPriority { name, value }),
        Err(_) => Ok(Some(CasaPriority::Warn)),
    }
}

fn priority_is_off(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "off" | "none" | "quiet" | "false" | "0"
    )
}

/// Parse and remove shared CASA logging flags from an argv vector.
pub fn configure_from_env_and_args(
    args: impl IntoIterator<Item = OsString>,
) -> Result<(Vec<OsString>, CasaLoggingConfig), CasaLoggingError> {
    apply_cli_logging_args(args, CasaLoggingConfig::from_env()?)
}

/// Remove shared CASA logging flags without reading environment or initializing logging.
///
/// Schema-driven callers use this before passing task arguments into a parser
/// that only understands task-specific controls.
pub fn strip_cli_logging_args(
    args: impl IntoIterator<Item = OsString>,
) -> Result<Vec<OsString>, CasaLoggingError> {
    let (filtered, _) = apply_cli_logging_args(args, CasaLoggingConfig::fallback())?;
    Ok(filtered)
}

fn apply_cli_logging_args(
    args: impl IntoIterator<Item = OsString>,
    mut config: CasaLoggingConfig,
) -> Result<(Vec<OsString>, CasaLoggingConfig), CasaLoggingError> {
    let mut filtered = Vec::new();
    let mut args = args.into_iter().peekable();
    while let Some(arg) = args.next() {
        if let Some(value) = split_option_value(&arg, LOG_TABLE_FLAG)? {
            config.table_path = Some(PathBuf::from(value));
            continue;
        }
        if arg == LOG_TABLE_FLAG {
            let value = next_option_value(&mut args, LOG_TABLE_FLAG)?;
            config.table_path = Some(PathBuf::from(value));
            continue;
        }
        if let Some(value) = split_option_value(&arg, LOG_TABLE_PRIORITY_FLAG)? {
            config.table_priority = CasaPriority::from_str(&value)?;
            continue;
        }
        if arg == LOG_TABLE_PRIORITY_FLAG {
            let value = next_option_value(&mut args, LOG_TABLE_PRIORITY_FLAG)?;
            config.table_priority = parse_priority_option(&value, LOG_TABLE_PRIORITY_FLAG)?;
            continue;
        }
        if let Some(value) = split_option_value(&arg, LOG_STDERR_PRIORITY_FLAG)? {
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

fn split_option_value(
    arg: &OsString,
    flag: &'static str,
) -> Result<Option<String>, CasaLoggingError> {
    let Some(arg) = arg.to_str() else {
        return Ok(None);
    };
    let Some(rest) = arg.strip_prefix(flag) else {
        return Ok(None);
    };
    if let Some(value) = rest.strip_prefix('=') {
        return Ok(Some(value.to_string()));
    }
    Ok(None)
}

fn next_option_value(
    args: &mut impl Iterator<Item = OsString>,
    flag: &'static str,
) -> Result<OsString, CasaLoggingError> {
    args.next()
        .ok_or(CasaLoggingError::MissingOptionValue(flag))
}

fn parse_optional_priority(value: &OsString) -> Result<Option<CasaPriority>, CasaLoggingError> {
    let value = value.to_str().ok_or(CasaLoggingError::NonUtf8OptionValue(
        LOG_STDERR_PRIORITY_FLAG,
    ))?;
    parse_optional_priority_str(value)
}

fn parse_optional_priority_str(value: &str) -> Result<Option<CasaPriority>, CasaLoggingError> {
    if priority_is_off(value) {
        Ok(None)
    } else {
        CasaPriority::from_str(value).map(Some)
    }
}

fn parse_priority_option(
    value: &OsString,
    flag: &'static str,
) -> Result<CasaPriority, CasaLoggingError> {
    let value = value
        .to_str()
        .ok_or(CasaLoggingError::NonUtf8OptionValue(flag))?;
    CasaPriority::from_str(value)
}

/// Configure global application logging and return the filtered argv.
pub fn init_global_from_env_and_args(
    args: impl IntoIterator<Item = OsString>,
) -> Result<(CasaLoggingGuard, Vec<OsString>), CasaLoggingError> {
    let (filtered_args, config) = configure_from_env_and_args(args)?;
    let guard = init_global(config)?;
    Ok((guard, filtered_args))
}

/// Guard holding sink state for explicit flush at task exit.
#[derive(Clone)]
pub struct CasaLoggingGuard {
    sink: Arc<dyn CasaLogSink>,
}

impl CasaLoggingGuard {
    pub fn flush(&self) -> Result<(), CasaLoggingError> {
        self.sink.flush()
    }
}

impl Drop for CasaLoggingGuard {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

/// `tracing_subscriber` layer that converts events into CASA log records.
#[derive(Clone)]
pub struct CasaLogLayer {
    sink: Arc<dyn CasaLogSink>,
}

impl CasaLogLayer {
    pub fn new(sink: Arc<dyn CasaLogSink>) -> Self {
        Self { sink }
    }
}

impl<S> Layer<S> for CasaLogLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let metadata = event.metadata();
        let mut visitor = EventVisitor::default();
        event.record(&mut visitor);
        let priority = visitor
            .priority
            .and_then(|value| CasaPriority::from_str(&value).ok())
            .unwrap_or_else(|| CasaPriority::from_tracing_level(metadata.level()));
        if !self.sink.enabled(priority) {
            return;
        }
        let message = visitor.message.unwrap_or(visitor.fields);
        let location = visitor.location.unwrap_or_else(|| {
            event_location(
                metadata.target(),
                metadata.file(),
                metadata.line(),
                ctx.lookup_current().map(|span| span.name()),
            )
        });
        let record = CasaLogRecord {
            time_mjd_seconds: system_time_to_mjd_seconds(SystemTime::now()),
            priority,
            message: trim_message(message),
            location,
            object_id: visitor.object_id.unwrap_or_default(),
        };
        if let Err(error) = self.sink.write(&record) {
            eprintln!("casa logging sink error: {error}");
        }
    }
}

fn event_location(
    target: &str,
    file: Option<&str>,
    line: Option<u32>,
    span: Option<&str>,
) -> String {
    let mut location = String::new();
    if let Some(span) = span {
        let _ = write!(location, "{span}::");
    }
    location.push_str(target);
    if let Some(file) = file {
        match line {
            Some(line) => {
                let _ = write!(location, " (file {file}, line {line})");
            }
            None => {
                let _ = write!(location, " (file {file})");
            }
        }
    }
    location
}

#[derive(Default)]
struct EventVisitor {
    message: Option<String>,
    priority: Option<String>,
    location: Option<String>,
    object_id: Option<String>,
    fields: String,
}

impl EventVisitor {
    fn push_field(&mut self, name: &str, value: impl fmt::Display) {
        if !self.fields.is_empty() {
            self.fields.push(' ');
        }
        let _ = write!(self.fields, "{name}={value}");
    }
}

impl Visit for EventVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        let rendered = format!("{value:?}");
        match field.name() {
            "message" => self.message = Some(rendered.trim_matches('"').to_string()),
            "casa.priority" => self.priority = Some(rendered.trim_matches('"').to_string()),
            "casa.location" => self.location = Some(rendered.trim_matches('"').to_string()),
            "casa.object_id" => self.object_id = Some(rendered.trim_matches('"').to_string()),
            name => self.push_field(name, rendered),
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "message" => self.message = Some(value.to_string()),
            "casa.priority" => self.priority = Some(value.to_string()),
            "casa.location" => self.location = Some(value.to_string()),
            "casa.object_id" => self.object_id = Some(value.to_string()),
            name => self.push_field(name, value),
        }
    }
}

/// Initialize global logging once for an application process.
pub fn init_global(config: CasaLoggingConfig) -> Result<CasaLoggingGuard, CasaLoggingError> {
    let mut sinks: Vec<Arc<dyn CasaLogSink>> = Vec::new();
    if let Some(path) = config.table_path {
        sinks.push(Arc::new(CasaTableLogSink::new(
            path,
            config.table_priority,
            TableLogOpenMode::CreateOrAppend,
        )));
    }
    if let Some(priority) = config.stderr_priority {
        sinks.push(Arc::new(StderrSink::new(priority)));
    }
    let sink: Arc<dyn CasaLogSink> = if sinks.is_empty() {
        Arc::new(NullSink)
    } else {
        Arc::new(CompositeSink::new(sinks))
    };
    let layer = CasaLogLayer::new(sink.clone());
    tracing::subscriber::set_global_default(Registry::default().with(layer))
        .map_err(|error| CasaLoggingError::Subscriber(error.to_string()))?;
    Ok(CasaLoggingGuard { sink })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
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
    fn priority_strings_round_trip() {
        for priority in CasaPriority::ALL {
            assert_eq!(
                CasaPriority::from_str(priority.as_casa_str()).unwrap(),
                priority
            );
        }
    }

    #[test]
    fn priority_threshold_order_matches_casa() {
        assert!(CasaPriority::Warn >= CasaPriority::Info);
        assert!(CasaPriority::Severe >= CasaPriority::Warn);
        assert!(CasaPriority::Info < CasaPriority::Severe);
        assert!(CasaPriority::Debugging < CasaPriority::Debug2);
    }

    #[test]
    fn tracing_level_default_mapping() {
        assert_eq!(
            CasaPriority::from_tracing_level(&Level::ERROR),
            CasaPriority::Severe
        );
        assert_eq!(
            CasaPriority::from_tracing_level(&Level::WARN),
            CasaPriority::Warn
        );
        assert_eq!(
            CasaPriority::from_tracing_level(&Level::INFO),
            CasaPriority::Info
        );
        assert_eq!(
            CasaPriority::from_tracing_level(&Level::DEBUG),
            CasaPriority::Debug1
        );
        assert_eq!(
            CasaPriority::from_tracing_level(&Level::TRACE),
            CasaPriority::Debugging
        );
    }

    #[test]
    fn unix_epoch_mjd_seconds_matches_casa_units() {
        assert_eq!(
            system_time_to_mjd_seconds(UNIX_EPOCH),
            UNIX_EPOCH_MJD_DAYS * SECONDS_PER_DAY
        );
    }

    #[test]
    fn message_trims_trailing_newlines() {
        let record = CasaLogRecord::new(CasaPriority::Info, "hello\n\n", "test");
        assert_eq!(record.message, "hello");
    }

    #[test]
    fn cli_logging_args_are_stripped_and_applied() {
        let _guard = env_lock();
        clear_log_env();
        let (filtered, config) = configure_from_env_and_args([
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
        let _guard = env_lock();
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
        let _guard = env_lock();
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
        let _guard = env_lock();
        clear_log_env();
        unsafe {
            std::env::set_var("CASA_RS_LOG_TABLE_PRIORITY", "loud");
        }
        assert!(matches!(
            CasaLoggingConfig::from_env(),
            Err(CasaLoggingError::InvalidEnvPriority {
                name: "CASA_RS_LOG_TABLE_PRIORITY",
                ..
            })
        ));
        clear_log_env();
    }

    #[test]
    fn creates_and_reads_canonical_log_table() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("casa.log");
        let sink =
            CasaTableLogSink::new(&path, CasaPriority::Info, TableLogOpenMode::CreateOrAppend);
        let record = CasaLogRecord::new(CasaPriority::Info, "Task started", "test::task")
            .with_object_id("object-1");
        sink.write(&record).unwrap();
        sink.flush().unwrap();
        let append_sink =
            CasaTableLogSink::new(&path, CasaPriority::Info, TableLogOpenMode::CreateOrAppend);
        append_sink
            .write(&CasaLogRecord::new(
                CasaPriority::Warn,
                "Task warning",
                "test::task",
            ))
            .unwrap();
        append_sink.flush().unwrap();

        let reopened = Table::open(TableOptions::new(&path)).unwrap();
        validate_log_table(&reopened).unwrap();
        assert_eq!(reopened.info().table_type, "LOG");
        assert_eq!(reopened.info().readme, vec![LOG_TABLE_README.to_string()]);
        let schema = reopened.schema().unwrap();
        assert_eq!(
            schema.column("TIME").unwrap().comment(),
            TIME_COLUMN_COMMENT
        );
        assert_eq!(schema.column("PRIORITY").unwrap().max_length(), 9);
        let time_keywords = reopened.column_keywords("TIME").unwrap();
        assert!(matches!(
            time_keywords.get("UNIT"),
            Some(Value::Scalar(ScalarValue::String(unit))) if unit == "s"
        ));
        assert_eq!(reopened.row_count(), 2);
    }

    #[test]
    fn rejects_missing_required_log_column() {
        let schema = casa_tables::TableSchema::new(vec![ColumnSchema::scalar(
            "TIME",
            PrimitiveType::Float64,
        )])
        .unwrap();
        let mut table = Table::with_schema(schema);
        table.set_info(TableInfo {
            table_type: "LOG".to_string(),
            sub_type: String::new(),
            readme: Vec::new(),
        });
        let err = validate_log_table(&table).unwrap_err();
        assert!(matches!(err, CasaLoggingError::MissingColumn("PRIORITY")));
    }

    #[test]
    fn rejects_non_log_table_info() {
        let mut table = create_log_table();
        table.set_info(TableInfo {
            table_type: "MeasurementSet".to_string(),
            sub_type: String::new(),
            readme: Vec::new(),
        });
        let err = validate_log_table(&table).unwrap_err();
        assert!(matches!(
            err,
            CasaLoggingError::IncompatibleTableInfo(table_type) if table_type == "MeasurementSet"
        ));
    }

    #[test]
    fn memory_sink_filters_by_threshold() {
        let sink = MemorySink::new(CasaPriority::Warn);
        sink.write(&CasaLogRecord::new(CasaPriority::Info, "skip", "test"))
            .unwrap();
        sink.write(&CasaLogRecord::new(CasaPriority::Severe, "keep", "test"))
            .unwrap();
        let records = sink.records().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].priority, CasaPriority::Severe);
    }
}
