// SPDX-License-Identifier: LGPL-3.0-or-later
//! Lex the parameter assignments used in CASA flag command files.

use casa_contracts::ParameterValue;

#[derive(Default)]
pub(super) struct Assignments(Vec<(String, ParameterValue, Option<String>)>);

impl Assignments {
    pub(super) fn get(&self, name: &str) -> Option<&ParameterValue> {
        self.0
            .iter()
            .find(|(key, _, _)| key == name)
            .map(|(_, value, _)| value)
    }

    pub(super) fn set(&mut self, name: &str, value: ParameterValue) {
        self.set_with_spelling(name, value, None);
    }

    pub(super) fn set_with_spelling(
        &mut self,
        name: &str,
        value: ParameterValue,
        spelling: Option<String>,
    ) {
        if let Some((_, previous, old_spelling)) = self.0.iter_mut().find(|(key, _, _)| key == name)
        {
            *previous = value;
            *old_spelling = spelling;
        } else {
            self.0.push((name.to_owned(), value, spelling));
        }
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&String, &ParameterValue)> {
        self.0.iter().map(|(key, value, _)| (key, value))
    }

    pub(super) fn spelling(&self, name: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(key, _, _)| key == name)
            .and_then(|(_, _, spelling)| spelling.as_deref())
    }
}

pub(super) fn parse_assignments(line: &str) -> Option<Assignments> {
    let mut fields: Vec<(&str, String)> = Vec::new();
    // CASA splits at spaces first and attaches tokens without an equals sign
    // to the preceding value, including spaces inside strings and lists.
    for word in line.trim().split(' ') {
        if let Some((key, value)) = word.split_once('=') {
            fields.push((key, value.to_owned()));
        } else {
            let (_, value) = fields.last_mut()?;
            value.push(' ');
            value.push_str(word);
        }
    }
    let mut values = Assignments::default();
    for (key, text) in fields {
        let value = match text.as_str() {
            "True" => ParameterValue::Bool(true),
            "False" => ParameterValue::Bool(false),
            text if text.starts_with(['[', '{']) => {
                let json = super::python_literal::to_json(text);
                let value = serde_json::from_str(&json).ok()?;
                ParameterValue::from_plain_json(&value)?
            }
            text => text
                .trim()
                .parse::<i64>()
                .map(ParameterValue::Integer)
                .or_else(|_| text.trim().parse::<f64>().map(ParameterValue::Float))
                .unwrap_or_else(|_| ParameterValue::String(command_string(text))),
        };
        values.set(key, value);
    }
    Some(values)
}

fn command_string(text: &str) -> String {
    let quote = if text.contains('\'') { '\'' } else { '"' };
    let text = text.trim_matches(quote);
    for quote in ['\'', '"'] {
        if text.matches(quote).count() == 1 {
            return text.replace(quote, "").trim().to_owned();
        }
    }
    text.trim().to_owned()
}
