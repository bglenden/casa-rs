// SPDX-License-Identifier: LGPL-3.0-or-later
//! Decode the Python literal spelling in CASA flagging files.

// Translate the Python literal spelling used in threshold records. The JSON
// decoder still checks the structure, and no expressions are evaluated.
pub(super) fn to_json(line: &str) -> String {
    let mut output = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    let mut quote = None;
    let mut previous = None;
    while let Some(character) = chars.next() {
        if let Some(delimiter) = quote {
            match character {
                '\\' => {
                    if let Some(escaped) = chars.next() {
                        if escaped != '\'' {
                            output.push('\\');
                        }
                        output.push(escaped);
                    } else {
                        output.push('\\');
                    }
                }
                character if character == delimiter => {
                    quote = None;
                    output.push('"');
                }
                '"' => output.push_str("\\\""),
                _ => output.push(character),
            }
        } else {
            match character {
                '\'' | '"' => {
                    quote = Some(character);
                    output.push('"');
                }
                '#' => break,
                ',' if matches!(
                    chars.clone().find(|ch| !ch.is_whitespace()),
                    Some(']' | '}')
                ) => {}
                '.' => {
                    if !previous.is_some_and(|ch: char| ch.is_ascii_digit()) {
                        output.push('0');
                    }
                    output.push('.');
                    if !chars.peek().is_some_and(char::is_ascii_digit) {
                        output.push('0');
                    }
                }
                '+' if !matches!(previous, Some('e' | 'E')) => {}
                _ => output.push(character),
            }
        }
        previous = Some(character);
    }
    output
}
