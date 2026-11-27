// SPDX-License-Identifier: LGPL-3.0-or-later

//! Error and log types for the pure-Rust image tool runtime.

use std::fmt;

use casa_components::ComponentError;
use casa_coordinates::CoordinateError;
use casa_core::measures::MeasureError;
use casa_core::quanta::{UnitError, casacore::QuantumError};
use casa_images::ImageError;
use casa_lattices::{LatticeError, RobustStatsError};
use casa_tables::TableError;
use ndarray::ShapeError;

/// Error returned by image-tool domain operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageToolError {
    message: String,
    not_implemented: Option<(&'static str, &'static str, u32)>,
}

impl ImageToolError {
    /// Creates an error from a diagnostic message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            not_implemented: None,
        }
    }

    /// Creates an unimplemented-feature marker carrying CASA tracking issue metadata.
    pub fn not_implemented(area: &'static str, feature: &'static str, issue: u32) -> Self {
        Self {
            message: format!("{feature} is not yet implemented (#{issue})"),
            not_implemented: Some((area, feature, issue)),
        }
    }

    /// Returns `(area, feature, issue)` if this error marks an unimplemented feature.
    pub fn not_implemented_info(&self) -> Option<(&'static str, &'static str, u32)> {
        self.not_implemented
    }

    /// Returns the diagnostic message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ImageToolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ImageToolError {}

/// Result type for image-tool domain operations.
pub type ImageToolResult<T> = Result<T, ImageToolError>;

impl From<String> for ImageToolError {
    fn from(message: String) -> Self {
        Self::new(message)
    }
}

impl From<&str> for ImageToolError {
    fn from(message: &str) -> Self {
        Self::new(message)
    }
}

macro_rules! impl_from_display {
    ($($error:ty),+ $(,)?) => {
        $(impl From<$error> for ImageToolError {
            fn from(error: $error) -> Self {
                Self::new(error.to_string())
            }
        })+
    };
}

impl_from_display!(
    ImageError,
    std::sync::Arc<ImageError>,
    TableError,
    CoordinateError,
    ComponentError,
    LatticeError,
    RobustStatsError,
    MeasureError,
    UnitError,
    QuantumError,
    ShapeError,
    std::io::Error,
    std::num::ParseIntError,
    std::num::TryFromIntError,
    std::str::Utf8Error,
    tempfile::PersistError,
);

/// Severity level for diagnostic log entries emitted by image-tool calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolLogLevel {
    /// Informational message (`INFO`).
    Info,
    /// Warning message (`WARN`).
    Warning,
    /// Error message (`SEVERE`).
    Error,
    /// Debug message (`DEBUG`).
    Debug,
}

/// A diagnostic log message deferred until tool locks are released.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolLogLine {
    /// Log severity level.
    pub level: ToolLogLevel,
    /// Formatted log text.
    pub message: String,
}

impl ToolLogLine {
    /// Creates an informational log line.
    pub fn info(message: impl Into<String>) -> Self {
        Self {
            level: ToolLogLevel::Info,
            message: message.into(),
        }
    }

    /// Creates a warning log line.
    pub fn warning(message: impl Into<String>) -> Self {
        Self {
            level: ToolLogLevel::Warning,
            message: message.into(),
        }
    }

    /// Creates an error log line.
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            level: ToolLogLevel::Error,
            message: message.into(),
        }
    }

    /// Creates a debug log line.
    pub fn debug(message: impl Into<String>) -> Self {
        Self {
            level: ToolLogLevel::Debug,
            message: message.into(),
        }
    }
}
