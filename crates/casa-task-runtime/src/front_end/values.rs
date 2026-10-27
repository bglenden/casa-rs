// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed reads of a resolved run, for a binary building its own request.
//!
//! The session has already parsed, normalized and validated every value
//! against its concept, so a read that finds the wrong type means the
//! binary and its surface disagree; it is reported as a refusal naming the
//! parameter rather than a panic.

use std::path::PathBuf;

use casa_provider_contracts::ParameterValue;

use super::{TaskFailure, TaskInvocation};
use crate::invocation::parameter_value_is_omitted;

impl TaskInvocation<'_> {
    /// The active value of `name`, or `None` when it is unset or inactive.
    pub fn value(&self, name: &str) -> Option<&ParameterValue> {
        let state = self.session.states().get(name)?;
        state.value.as_ref().filter(|_| state.active)
    }

    /// `name` as text, or `None` when it is unset, inactive, or holds the
    /// explicit absence its provider adapter omits (`"none"`).
    pub fn text(&self, name: &str) -> Result<Option<String>, TaskFailure> {
        let Some(value) = self.value(name) else {
            return Ok(None);
        };
        let adapter = self
            .session
            .bundle()
            .surface
            .bindings()
            .iter()
            .find(|binding| binding.name == name)
            .and_then(|binding| binding.projections.provider.as_ref())
            .map(|projection| &projection.adapter);
        if parameter_value_is_omitted(value, adapter) {
            return Ok(None);
        }
        match value {
            ParameterValue::String(text) => Ok(Some(text.clone())),
            other => Err(mismatch(name, "text", other)),
        }
    }

    /// Whether the run sets `name`, from its base profile or an override,
    /// rather than taking the default.
    pub fn is_set(&self, name: &str) -> bool {
        self.session
            .states()
            .get(name)
            .is_some_and(|state| state.active && state.explicit)
    }

    /// `name` as text, which must be set.
    pub fn required_text(&self, name: &str) -> Result<String, TaskFailure> {
        self.text(name)?.ok_or_else(|| missing(name))
    }

    /// `name` as paths: one path, or a list of them.
    pub fn paths(&self, name: &str) -> Result<Vec<PathBuf>, TaskFailure> {
        match self.value(name) {
            None => Ok(Vec::new()),
            Some(ParameterValue::String(path)) => Ok(vec![PathBuf::from(path)]),
            Some(ParameterValue::Array(items)) => items
                .iter()
                .map(|item| match item {
                    ParameterValue::String(path) => Ok(PathBuf::from(path)),
                    other => Err(mismatch(name, "a path", other)),
                })
                .collect(),
            Some(other) => Err(mismatch(name, "a path or a list of paths", other)),
        }
    }

    /// `name` as exactly one path.
    pub fn path(&self, name: &str) -> Result<PathBuf, TaskFailure> {
        let mut paths = self.paths(name)?;
        match paths.len() {
            1 => Ok(paths.remove(0)),
            0 => Err(missing(name)),
            count => Err(TaskFailure::validation(format!(
                "parameter {name:?} names {count} paths; this task takes one"
            ))
            .at(name)),
        }
    }

    /// `name` as a flag.
    pub fn flag(&self, name: &str) -> Result<bool, TaskFailure> {
        match self.value(name) {
            Some(ParameterValue::Bool(value)) => Ok(*value),
            Some(other) => Err(mismatch(name, "a flag", other)),
            None => Err(missing(name)),
        }
    }

    /// `name` as an integer.
    pub fn integer(&self, name: &str) -> Result<i64, TaskFailure> {
        match self.value(name) {
            Some(ParameterValue::Integer(value)) => Ok(*value),
            Some(other) => Err(mismatch(name, "an integer", other)),
            None => Err(missing(name)),
        }
    }

    /// `name` as a number; an integer value is read as one.
    pub fn number(&self, name: &str) -> Result<f64, TaskFailure> {
        match self.value(name) {
            Some(ParameterValue::Float(value)) => Ok(*value),
            Some(ParameterValue::Integer(value)) => Ok(*value as f64),
            Some(other) => Err(mismatch(name, "a number", other)),
            None => Err(missing(name)),
        }
    }
}

fn missing(name: &str) -> TaskFailure {
    TaskFailure::validation(format!("parameter {name:?} is required")).at(name)
}

fn mismatch(name: &str, expected: &str, found: &ParameterValue) -> TaskFailure {
    TaskFailure::validation(format!(
        "parameter {name:?} must be {expected}, found {found:?}"
    ))
    .at(name)
}
