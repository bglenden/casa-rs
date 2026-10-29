// SPDX-License-Identifier: LGPL-3.0-or-later
//! The list transform of a profile migration,
//! [`MigrationTransform::TextList`](super::MigrationTransform::TextList)
//! (#931).
//!
//! casa-rs's old calibration surfaces took CASA's lists as text: the
//! fields of `reference_fields="3C286,J1331"`, a model `smodel="1,0,0,0"`,
//! and per-table chains whose items are `;`-separated, such as
//! `spwmap="0,0;1,1"`. CASA takes lists. A migration reads the old text
//! into the list it meant; the old explicit absence, `"none"` or empty, is
//! the empty list.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::ParameterValue;

/// How each item of an old text list is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ListElement {
    /// The item as text, trimmed; an empty item stays, since a per-table
    /// list is positional (`gainfield="0;;2"`).
    String,
    /// The item as an integer.
    Integer,
    /// The item as a number.
    Float,
    /// The item as a flag: `true`, `t`, `1`, `yes` or `y`, or their
    /// opposites, as the old surfaces read them.
    Bool,
    /// The item as a `,`-separated list of integers, in brackets or not:
    /// one table's old `spwmap` (`"[0,0,1]"`); an empty item is the empty
    /// list.
    IntegerList,
}

impl ListElement {
    /// `text` as a list of this element: its `separator`-separated items,
    /// each read as one. `"none"` or empty text is the empty list.
    pub fn parse_list(self, text: &str, separator: &str) -> Result<ParameterValue, String> {
        let text = text.trim();
        if text.is_empty() || text.eq_ignore_ascii_case("none") {
            return Ok(ParameterValue::Array(Vec::new()));
        }
        text.split(separator)
            .map(|item| self.parse_item(item.trim()))
            .collect::<Result<Vec<_>, _>>()
            .map(ParameterValue::Array)
    }

    fn parse_item(self, item: &str) -> Result<ParameterValue, String> {
        let number = |item: &str| {
            item.parse::<f64>()
                .map_err(|error| format!("{item:?} is not a number: {error}"))
        };
        match self {
            Self::String => Ok(ParameterValue::String(item.to_string())),
            Self::Integer => item
                .parse::<i64>()
                .map(ParameterValue::Integer)
                .map_err(|error| format!("{item:?} is not an integer: {error}")),
            Self::Float => number(item).map(ParameterValue::Float),
            Self::Bool => match item.to_ascii_lowercase().as_str() {
                "true" | "t" | "1" | "yes" | "y" => Ok(ParameterValue::Bool(true)),
                "false" | "f" | "0" | "no" | "n" => Ok(ParameterValue::Bool(false)),
                _ => Err(format!("{item:?} is not true or false")),
            },
            Self::IntegerList => {
                let inner = item
                    .strip_prefix('[')
                    .and_then(|item| item.strip_suffix(']'))
                    .unwrap_or(item);
                inner
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(|value| Self::Integer.parse_item(value))
                    .collect::<Result<Vec<_>, _>>()
                    .map(ParameterValue::Array)
            }
        }
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

    fn integers(values: &[i64]) -> ParameterValue {
        ParameterValue::Array(
            values
                .iter()
                .copied()
                .map(ParameterValue::Integer)
                .collect(),
        )
    }

    #[test]
    fn old_text_reads_as_the_list_it_meant() {
        let list = |element: ListElement, text, separator| element.parse_list(text, separator);
        assert_eq!(
            list(ListElement::String, "3C286, J1331", ",").unwrap(),
            strings(&["3C286", "J1331"])
        );
        // A per-table list is positional: the empty item stays.
        assert_eq!(
            list(ListElement::String, "0;;nearest", ";").unwrap(),
            strings(&["0", "", "nearest"])
        );
        assert_eq!(
            list(ListElement::Float, "1,0,0,0", ",").unwrap(),
            ParameterValue::Array(vec![
                ParameterValue::Float(1.0),
                ParameterValue::Float(0.0),
                ParameterValue::Float(0.0),
                ParameterValue::Float(0.0),
            ])
        );
        assert_eq!(
            list(ListElement::Bool, "true, f", ",").unwrap(),
            ParameterValue::Array(vec![
                ParameterValue::Bool(true),
                ParameterValue::Bool(false)
            ])
        );
        assert_eq!(
            list(ListElement::IntegerList, "[0,0,1]; 1,1 ;", ";").unwrap(),
            ParameterValue::Array(vec![integers(&[0, 0, 1]), integers(&[1, 1]), integers(&[])])
        );
        assert_eq!(
            list(ListElement::Integer, "0, 2", ",").unwrap(),
            integers(&[0, 2])
        );
        for absent in ["none", " ", "None"] {
            assert_eq!(
                list(ListElement::IntegerList, absent, ";").unwrap(),
                ParameterValue::Array(Vec::new())
            );
        }
        assert!(list(ListElement::Integer, "0,x", ",").is_err());
        assert!(list(ListElement::Bool, "maybe", ",").is_err());
    }
}
