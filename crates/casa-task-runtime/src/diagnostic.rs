// SPDX-License-Identifier: LGPL-3.0-or-later

use serde::{Deserialize, Serialize};

/// Severity of a parameter/profile diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticLevel {
    /// Informational resolution detail.
    Info,
    /// The profile is usable but deserves user attention.
    Warning,
    /// Resolution or validation cannot continue.
    Error,
}

/// Stable machine-readable diagnostic category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticCode {
    UnknownParameter,
    DuplicateParameter,
    InactiveParameter,
    WrongSurface,
    WrongKind,
    FutureFormat,
    FutureContract,
    MissingRequired,
    InvalidType,
    InvalidValue,
    UnsafeValue,
    DefaultChanged,
    Migrated,
    AutomaticSaveFailed,
}

/// One-based source location within a human profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceLocation {
    pub line: usize,
    pub column: usize,
}

/// One actionable profile diagnostic shared by every UI surface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub level: DiagnosticLevel,
    pub code: DiagnosticCode,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameter: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<SourceLocation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub suggestions: Vec<String>,
}

impl Diagnostic {
    pub(crate) fn error(code: DiagnosticCode, message: impl Into<String>) -> Self {
        Self {
            level: DiagnosticLevel::Error,
            code,
            message: message.into(),
            parameter: None,
            location: None,
            suggestions: Vec::new(),
        }
    }

    pub(crate) fn warning(code: DiagnosticCode, message: impl Into<String>) -> Self {
        Self {
            level: DiagnosticLevel::Warning,
            code,
            message: message.into(),
            parameter: None,
            location: None,
            suggestions: Vec::new(),
        }
    }

    pub(crate) fn for_parameter(mut self, parameter: impl Into<String>) -> Self {
        self.parameter = Some(parameter.into());
        self
    }
}
