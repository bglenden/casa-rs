// SPDX-License-Identifier: LGPL-3.0-or-later
//! The list migrations of a saved profile (#931): an old text list read
//! into CASA's list ([`MigrationTransform::TextList`]), and an old
//! one-value-for-every-table list repeated for each table
//! ([`MigrationStep::Broadcast`]).
//!
//! [`MigrationTransform::TextList`]: casa_provider_contracts::MigrationTransform::TextList
//! [`MigrationStep::Broadcast`]: casa_provider_contracts::MigrationStep::Broadcast

use std::collections::BTreeMap;

use casa_provider_contracts::{ListElement, ParameterValue};

use crate::profile::ProfileError;

/// `value`, old text, as the list of `element` it meant; a value that is
/// not text was saved by a newer contract and stays.
pub(crate) fn text_list(
    value: ParameterValue,
    separator: &str,
    element: ListElement,
    parameter: &str,
) -> Result<ParameterValue, ProfileError> {
    let ParameterValue::String(text) = value else {
        return Ok(value);
    };
    element.parse_list(&text, separator).map_err(|error| {
        ProfileError::Contract(format!(
            "migration could not read {parameter:?} = {text:?} as a list: {error}"
        ))
    })
}

/// Repeat the one item of the explicit list `parameter` for each item of
/// the explicit list `to_length_of`. Anything else stays: a list of
/// several items already names each table.
pub(crate) fn broadcast(
    parameters: &mut BTreeMap<String, ParameterValue>,
    parameter: &str,
    to_length_of: &str,
) {
    let Some(ParameterValue::Array(tables)) = parameters.get(to_length_of) else {
        return;
    };
    let count = tables.len();
    if let Some(ParameterValue::Array(items)) = parameters.get_mut(parameter)
        && items.len() == 1
        && count > 1
    {
        let item = items[0].clone();
        items.resize(count, item);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(values: &[&str]) -> ParameterValue {
        ParameterValue::Array(
            values
                .iter()
                .map(|value| ParameterValue::String((*value).into()))
                .collect(),
        )
    }

    #[test]
    fn one_old_value_names_every_table() {
        let mut parameters = BTreeMap::from([
            (
                "gaintable".to_string(),
                strings(&["a.cal", "b.cal", "c.cal"]),
            ),
            ("interp".to_string(), strings(&["nearest"])),
            ("gainfield".to_string(), strings(&["0", "1"])),
        ]);
        broadcast(&mut parameters, "interp", "gaintable");
        broadcast(&mut parameters, "gainfield", "gaintable");
        broadcast(&mut parameters, "spwmap", "gaintable");
        assert_eq!(
            parameters["interp"],
            strings(&["nearest", "nearest", "nearest"])
        );
        // A positional list, and a list never set, stay as they are.
        assert_eq!(parameters["gainfield"], strings(&["0", "1"]));
        assert!(!parameters.contains_key("spwmap"));
    }

    #[test]
    fn old_text_becomes_a_list_and_a_list_stays() {
        let read = |value| text_list(value, ";", ListElement::String, "gainfield");
        assert_eq!(
            read(ParameterValue::String("0;nearest".into())).unwrap(),
            strings(&["0", "nearest"])
        );
        assert_eq!(read(strings(&["0"])).unwrap(), strings(&["0"]));
        let error = text_list(
            ParameterValue::String("x".into()),
            ",",
            ListElement::Integer,
            "refspwmap",
        )
        .unwrap_err();
        assert!(error.to_string().contains("refspwmap"), "{error}");
    }
}
