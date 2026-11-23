// SPDX-License-Identifier: LGPL-3.0-or-later
//! CalLibraryGram's typed tokens and comma/newline entry boundaries.
use std::collections::BTreeMap;

pub(super) struct Entry {
    pub line: usize,
    pub values: BTreeMap<String, String>,
}

enum Token {
    String(String),
    Bool(bool),
    Array(Vec<i32>),
    Equals,
    End,
}

const PARAMETERS: &[&str] = &[
    "field", "intent", "spw", "obs", "scan", "tinterp", "finterp", "reach", "antmap", "fldmap",
    "obsmap", "scanmap", "spwmap",
];

pub(super) fn parse(
    contents: &str,
    allow_empty_maps: bool,
) -> Result<(Vec<Entry>, bool), (usize, String)> {
    let tokens = lex(contents, allow_empty_maps)?;
    let mut entries = Vec::new();
    let mut has_statement = false;
    let mut values = BTreeMap::new();
    let mut index = 0;
    while index < tokens.len() {
        let (line, token) = &tokens[index];
        if matches!(token, Token::End) {
            if values
                .get("caltable")
                .is_some_and(|name: &String| name.len() > 2)
            {
                entries.push(Entry {
                    line: *line,
                    values: std::mem::take(&mut values),
                });
            }
            index += 1;
            continue;
        }
        let Token::String(key) = token else {
            return Err((*line, "expected parameter name".into()));
        };
        has_statement = true;
        if !matches!(tokens.get(index + 1), Some((_, Token::Equals))) {
            return Err((*line, "expected '=' after parameter name".into()));
        }
        let Some((_, value)) = tokens.get(index + 2) else {
            return Err((*line, "missing parameter value".into()));
        };
        match value {
            Token::String(value) if key == "caltable" || PARAMETERS.contains(&key.as_str()) => {
                // addStringParam changes embedded single quotes to double quotes.
                values.insert(
                    key.clone(),
                    format!(
                        "\"{}\"",
                        if key == "caltable" {
                            value.clone()
                        } else {
                            value.replace('\'', "\"")
                        }
                    ),
                );
            }
            Token::Bool(value) if key == "calwt" => {
                values.insert(key.clone(), value.to_string());
            }
            Token::Array(ids) if PARAMETERS.contains(&key.as_str()) => {
                if !["antmap", "fldmap", "obsmap", "scanmap", "spwmap"].contains(&key.as_str())
                    || values.get(key).is_some_and(|value| !value.starts_with('['))
                {
                    return Err((
                        *line,
                        format!("record field {key} has a string type, not an integer array"),
                    ));
                }
                values.insert(
                    key.clone(),
                    format!(
                        "[{}]",
                        ids.iter().map(i32::to_string).collect::<Vec<_>>().join(",")
                    ),
                );
            }
            Token::String(_) | Token::Bool(_) | Token::Array(_) => {
                tracing::warn!(
                    keyword = key,
                    "Cal Library: undefined keyword or value type will be ignored"
                );
            }
            _ => return Err((*line, "invalid parameter value".into())),
        }
        index += 3;
    }
    // CalLibraryTools appends a newline even to an unterminated final file line.
    if values.get("caltable").is_some_and(|name| name.len() > 2) {
        entries.push(Entry {
            line: contents.lines().count().max(1),
            values,
        });
    }
    Ok((entries, has_statement))
}

fn lex(contents: &str, allow_empty_maps: bool) -> Result<Vec<(usize, Token)>, (usize, String)> {
    let chars = contents.chars().collect::<Vec<_>>();
    let mut tokens = Vec::new();
    let (mut index, mut line, mut beginning) = (0, 1, true);
    while index < chars.len() {
        let ch = chars[index];
        match ch {
            ' ' | '\t' | '\r' => {
                index += 1;
            }
            '#' if beginning => {
                while index < chars.len() && chars[index] != '\n' {
                    index += 1;
                }
            }
            ',' | '\n' => {
                tokens.push((line, Token::End));
                line += 1;
                beginning = ch == '\n';
                index += 1;
            }
            '=' => {
                tokens.push((line, Token::Equals));
                index += 1;
                beginning = false;
            }
            '\'' | '"' => {
                beginning = false;
                let start_line = line;
                let mut value = String::new();
                loop {
                    let quote = chars[index];
                    index += 1;
                    while index < chars.len() && chars[index] != quote && chars[index] != '\n' {
                        value.push(chars[index]);
                        index += 1;
                    }
                    if index == chars.len() || chars[index] != quote {
                        return Err((start_line, "unterminated quoted string".into()));
                    }
                    index += 1;
                    if !matches!(chars.get(index), Some('\'' | '"')) {
                        break;
                    }
                    value.push(quote);
                    value.push(chars[index]);
                }
                tokens.push((start_line, Token::String(value)));
            }
            '[' => {
                beginning = false;
                let start_line = line;
                index += 1;
                let start = index;
                let mut ids = Vec::new();
                while index < chars.len() && chars[index] != ']' {
                    if chars[index].is_ascii_digit() {
                        let start = index;
                        while index < chars.len() && chars[index].is_ascii_digit() {
                            index += 1;
                        }
                        let number = chars[start..index]
                            .iter()
                            .collect::<String>()
                            .parse::<i32>()
                            .map_err(|e| (line, e.to_string()))?;
                        ids.push(number);
                    } else {
                        if chars[index] == '\n' {
                            line += 1;
                        }
                        // ORDERED_LIST ignores delimiters and the lexer echoes
                        // other characters, including a minus sign, without a token.
                        index += 1;
                    }
                }
                if index == chars.len()
                    || (ids.is_empty()
                        && (!allow_empty_maps
                            || chars[start..index].iter().any(|ch| !ch.is_whitespace())))
                {
                    return Err((start_line, "empty or unterminated mapping array".into()));
                }
                index += 1;
                tokens.push((start_line, Token::Array(ids)));
            }
            ch if ch.is_ascii_alphanumeric() => {
                beginning = false;
                let start = index;
                while index < chars.len() && chars[index].is_ascii_alphanumeric() {
                    index += 1;
                }
                let word = chars[start..index].iter().collect::<String>();
                let token = match word.as_str() {
                    "True" | "T" | "true" => Token::Bool(true),
                    "False" | "F" | "false" => Token::Bool(false),
                    value if value.chars().all(|ch| ch.is_ascii_digit()) => {
                        return Err((
                            line,
                            "numeric scalar values are not in CalLibraryGram".into(),
                        ));
                    }
                    _ => Token::String(word),
                };
                tokens.push((line, token));
            }
            _ => {
                index += 1;
                beginning = false;
            }
        }
    }
    Ok(tokens)
}
