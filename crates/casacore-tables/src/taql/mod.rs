// SPDX-License-Identifier: LGPL-3.0-or-later
//! TaQL — Table Query Language for casacore tables.
//!
//! TaQL is casacore's SQL-like query language for filtering, projecting,
//! sorting, aggregating, and mutating table data. This module provides a
//! Rust implementation covering the most-used subset of TaQL, built on a
//! [`logos`]-generated lexer and hand-written Pratt parser.
//!
//! # Quick start
//!
//! ```rust
//! use casacore_tables::taql;
//!
//! // Parse a query into an AST:
//! let stmt = taql::parse("SELECT col1, col2 WHERE flux > 1.0").unwrap();
//! println!("{stmt}"); // round-trips back to TaQL text
//! ```
//!
//! For executing queries against a [`Table`](crate::Table), see
//! [`Table::query`](crate::Table::query) and
//! [`Table::execute_taql`](crate::Table::execute_taql).
//!
//! # Supported statements
//!
//! - `SELECT` — filter, project, sort, group, and aggregate
//! - `UPDATE` — modify cell values
//! - `INSERT` — add new rows
//! - `DELETE` — remove rows
//!
//! # Built-in functions
//!
//! ~35 built-in scalar functions are available: trigonometric, exponential,
//! rounding, string manipulation, type conversion, and array inspection.
//! See [`functions`] for the full list.
//!
//! # C++ reference
//!
//! The C++ TaQL implementation spans `tables/TaQL/` in the casacore source
//! tree, built on flex/bison with 220+ functions. This Rust implementation
//! covers the practical subset used by most astronomy pipelines.

pub mod ast;
pub mod error;
mod lexer;
pub mod token;

mod parser;

pub mod eval;
pub mod exec;

pub mod aggregate;
pub mod format;
pub mod functions;
mod meas_udf;

pub use ast::Statement;
pub use error::TaqlError;
pub use exec::TaqlResult;

/// Parses a TaQL query string into an AST [`Statement`].
///
/// This is the primary entry point for TaQL parsing. The returned AST
/// can be inspected, transformed, or passed to [`execute`] for evaluation
/// against a table.
///
/// # Errors
///
/// Returns [`TaqlError`] if the query is syntactically invalid.
///
/// # Examples
///
/// ```rust
/// use casacore_tables::taql;
///
/// let stmt = taql::parse("SELECT * WHERE flux > 1.0").unwrap();
/// ```
pub fn parse(query: &str) -> Result<Statement, TaqlError> {
    parser::Parser::new(query).parse_statement()
}

/// Executes a parsed TaQL statement against a table.
///
/// For convenience methods, see [`Table::query`](crate::Table::query) and
/// [`Table::execute_taql`](crate::Table::execute_taql).
///
/// # Errors
///
/// Returns [`TaqlError`] on type errors, missing columns, or table errors.
pub fn execute(stmt: &Statement, table: &mut crate::Table) -> Result<TaqlResult, TaqlError> {
    exec::execute(stmt, table)
}
