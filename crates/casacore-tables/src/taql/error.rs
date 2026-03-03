// SPDX-License-Identifier: LGPL-3.0-or-later
//! Error types for the TaQL parser and evaluator.
//!
//! All errors produced by lexing, parsing, evaluating, or executing TaQL
//! queries are represented by the [`TaqlError`] enum. Each variant carries
//! enough context to produce a useful diagnostic message.

use std::fmt;

/// Position in the TaQL source string (1-based line and column).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourcePos {
    /// 1-based line number.
    pub line: usize,
    /// 1-based column number (byte offset within the line).
    pub col: usize,
}

impl fmt::Display for SourcePos {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.line, self.col)
    }
}

/// All errors produced by the TaQL subsystem.
///
/// C++ casacore throws `TableError` or `TableGramError` for query failures.
/// This enum provides structured variants with source-position context.
#[derive(Debug, thiserror::Error)]
pub enum TaqlError {
    /// The lexer encountered an invalid token.
    #[error("lexer error at {pos}: {message}")]
    LexError { pos: SourcePos, message: String },

    /// The parser encountered unexpected input.
    #[error("parse error at {pos}: {message}")]
    ParseError { pos: SourcePos, message: String },

    /// The parser reached end of input unexpectedly.
    #[error("unexpected end of query: {message}")]
    UnexpectedEnd { message: String },

    /// An expression evaluated to an unexpected type.
    #[error("type error: {message}")]
    TypeError { message: String },

    /// A column referenced in a query does not exist.
    #[error("column not found: \"{name}\"")]
    ColumnNotFound { name: String },

    /// A function referenced in a query does not exist.
    #[error("unknown function: \"{name}\"")]
    UnknownFunction { name: String },

    /// A function was called with the wrong number of arguments.
    #[error("function \"{name}\" expects {expected} argument(s), got {got}")]
    ArgumentCount {
        name: String,
        expected: String,
        got: usize,
    },

    /// A feature is parsed but not yet implemented.
    #[error("unsupported: {message}")]
    Unsupported { message: String },

    /// An error from the underlying table operations.
    #[error("table error: {0}")]
    Table(String),

    /// Division by zero in expression evaluation.
    #[error("division by zero")]
    DivisionByZero,

    /// INSERT column count does not match value count.
    #[error("INSERT column count ({columns}) does not match value count ({values})")]
    InsertColumnMismatch { columns: usize, values: usize },

    /// A JOIN table could not be opened.
    #[error("cannot open JOIN table \"{name}\": {message}")]
    JoinTableOpen { name: String, message: String },
}

impl TaqlError {
    /// Convenience constructor for a parse error at a position.
    pub fn parse(pos: SourcePos, msg: impl Into<String>) -> Self {
        Self::ParseError {
            pos,
            message: msg.into(),
        }
    }

    /// Convenience constructor for an unexpected-end error.
    pub fn unexpected_end(msg: impl Into<String>) -> Self {
        Self::UnexpectedEnd {
            message: msg.into(),
        }
    }
}
