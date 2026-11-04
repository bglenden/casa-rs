// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's recorded single-type integer parameter refusals.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use casa_provider_contracts::ParameterValue;

/// Message for a value CASA's `cInt` validator refuses, where the pinned
/// task schema has that single type. Python booleans count as integers in
/// CASA; casa-rs currently refuses them, so they retain its own diagnostic.
pub(crate) fn integer_refusal(surface: &str, name: &str, value: &ParameterValue) -> Option<String> {
    if matches!(value, ParameterValue::Integer(_) | ParameterValue::Bool(_)) {
        return None;
    }
    static PARAMETERS: OnceLock<BTreeMap<String, Vec<String>>> = OnceLock::new();
    let parameters = PARAMETERS.get_or_init(|| {
        serde_json::from_str(include_str!("../resources/casa-integer-parameters.json"))
            .expect("the checked CASA integer-parameter inventory")
    });
    parameters
        .get(surface)
        .is_some_and(|names| names.iter().any(|item| item == name))
        .then(|| format!("{{'{name}': ['must be of cInt type']}}"))
}
