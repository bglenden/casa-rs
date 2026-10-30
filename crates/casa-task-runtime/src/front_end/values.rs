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

    /// The active value of `name`, or `None` when it is unset, inactive, or
    /// holds the explicit absence its provider adapter omits (`"none"`).
    fn present(&self, name: &str) -> Option<&ParameterValue> {
        let value = self.value(name)?;
        let adapter = self
            .session
            .bundle()
            .surface
            .bindings()
            .iter()
            .find(|binding| binding.name == name)
            .and_then(|binding| binding.projections.provider.as_ref())
            .map(|projection| &projection.adapter);
        (!parameter_value_is_omitted(value, adapter)).then_some(value)
    }

    /// `name` as text, or `None` when it is unset, inactive, or holds the
    /// explicit absence its provider adapter omits (`"none"`).
    pub fn text(&self, name: &str) -> Result<Option<String>, TaskFailure> {
        match self.present(name) {
            None => Ok(None),
            Some(ParameterValue::String(text)) => Ok(Some(text.clone())),
            Some(other) => Err(mismatch(name, "text", other)),
        }
    }

    /// `name` as an optional number, such as flagdata's `timedev`: `None`
    /// when it is unset, inactive, or holds the explicit absence its
    /// provider adapter omits (`"none"`); an integer value is read as one.
    pub fn optional_number(&self, name: &str) -> Result<Option<f64>, TaskFailure> {
        match self.present(name) {
            None => Ok(None),
            Some(ParameterValue::Float(value)) => Ok(Some(*value)),
            Some(ParameterValue::Integer(value)) => Ok(Some(*value as f64)),
            Some(other) => Err(mismatch(name, "a number", other)),
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

    /// `name` as paths: one path, or a list of them; none when it is unset,
    /// inactive, or holds the explicit absence its provider adapter omits
    /// (`"none"`).
    pub fn paths(&self, name: &str) -> Result<Vec<PathBuf>, TaskFailure> {
        match self.present(name) {
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

    /// `name` as exactly one path to an input that exists, relative to the
    /// workspace; a missing input is a refusal naming the parameter, before
    /// the task starts, as CASA's `imstat('missing.im')` is.
    pub fn input_path(&self, name: &str) -> Result<PathBuf, TaskFailure> {
        let path = self.path(name)?;
        self.existing(name, &path)?;
        Ok(path)
    }

    /// `name` as paths to inputs that exist, as [`input_path`](Self::input_path)
    /// reads one.
    pub fn input_paths(&self, name: &str) -> Result<Vec<PathBuf>, TaskFailure> {
        let paths = self.paths(name)?;
        for path in &paths {
            self.existing(name, path)?;
        }
        Ok(paths)
    }

    fn existing(&self, name: &str, path: &std::path::Path) -> Result<(), TaskFailure> {
        if self.workspace().join(path).exists() {
            return Ok(());
        }
        Err(TaskFailure::validation(format!("{name} {:?} does not exist", path.display())).at(name))
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

    /// `name` as a list of numbers, such as CASA's `includepix=[0.1, 10]`;
    /// one number is a list of one, and integers are read as numbers.
    pub fn numbers(&self, name: &str) -> Result<Vec<f64>, TaskFailure> {
        let number = |value: &ParameterValue| match value {
            ParameterValue::Float(value) => Ok(*value),
            ParameterValue::Integer(value) => Ok(*value as f64),
            other => Err(mismatch(name, "a list of numbers", other)),
        };
        match self.value(name) {
            Some(ParameterValue::Array(items)) => items.iter().map(number).collect(),
            Some(value) => Ok(vec![number(value)?]),
            None => Err(missing(name)),
        }
    }

    /// `name` as a list of integers, such as CASA's `moments=[0]`; one
    /// integer is a list of one.
    pub fn integers(&self, name: &str) -> Result<Vec<i64>, TaskFailure> {
        let integer = |value: &ParameterValue| match value {
            ParameterValue::Integer(value) => Ok(*value),
            other => Err(mismatch(name, "a list of integers", other)),
        };
        match self.value(name) {
            Some(ParameterValue::Array(items)) => items.iter().map(integer).collect(),
            Some(value) => Ok(vec![integer(value)?]),
            None => Err(missing(name)),
        }
    }

    /// `name` as a list of text, such as CASA's `gainfield=['', '0']`; one
    /// text is a list of one. Empty items are kept, since a list's
    /// positions can matter.
    pub fn texts(&self, name: &str) -> Result<Vec<String>, TaskFailure> {
        let text = |value: &ParameterValue| match value {
            ParameterValue::String(text) => Ok(text.clone()),
            other => Err(mismatch(name, "a list of text", other)),
        };
        match self.value(name) {
            Some(ParameterValue::Array(items)) => items.iter().map(text).collect(),
            Some(value) => Ok(vec![text(value)?]),
            None => Err(missing(name)),
        }
    }

    /// `name` as a list of flags, such as CASA's `calwt=[True]`; one flag
    /// is a list of one.
    pub fn flags(&self, name: &str) -> Result<Vec<bool>, TaskFailure> {
        let flag = |value: &ParameterValue| match value {
            ParameterValue::Bool(value) => Ok(*value),
            other => Err(mismatch(name, "a list of flags", other)),
        };
        match self.value(name) {
            Some(ParameterValue::Array(items)) => items.iter().map(flag).collect(),
            Some(value) => Ok(vec![flag(value)?]),
            None => Err(missing(name)),
        }
    }

    /// `name` as a list of integer lists, such as CASA's
    /// `spwmap=[[0, 0, 1], []]`.
    pub fn integer_lists(&self, name: &str) -> Result<Vec<Vec<i64>>, TaskFailure> {
        let integer = |value: &ParameterValue| match value {
            ParameterValue::Integer(value) => Ok(*value),
            other => Err(mismatch(name, "a list of integer lists", other)),
        };
        let list = |value: &ParameterValue| match value {
            ParameterValue::Array(items) => items.iter().map(integer).collect(),
            other => Err(mismatch(name, "a list of integer lists", other)),
        };
        match self.value(name) {
            Some(ParameterValue::Array(items)) => items.iter().map(list).collect(),
            Some(other) => Err(mismatch(name, "a list of integer lists", other)),
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
