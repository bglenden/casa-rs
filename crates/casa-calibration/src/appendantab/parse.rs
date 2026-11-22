// SPDX-License-Identifier: LGPL-3.0-or-later
//! ANTAB headers and their following TSYS sample blocks.

#[derive(Debug)]
pub(super) struct Antab<'a> {
    pub(super) gains: Vec<Header>,
    pub(super) tsys: Vec<(Header, Vec<&'a str>)>,
}

#[derive(Debug)]
pub(super) struct Header(Vec<(String, Field)>);

#[derive(Debug)]
enum Field {
    Switch,
    Values(Vec<String>),
}

impl Header {
    pub(super) fn antenna(&self, reserved: &[&str]) -> Option<&str> {
        self.0.iter().skip(1).find_map(|(key, value)| {
            (matches!(value, Field::Switch) && !reserved.contains(&key.as_str()))
                .then_some(key.as_str())
        })
    }

    pub(super) fn has(&self, key: &str) -> bool {
        self.0.iter().any(|(name, _)| name == key)
    }

    pub(super) fn values(&self, key: &str) -> Option<&[String]> {
        self.0.iter().rev().find_map(|(name, value)| match value {
            Field::Values(values) if name == key => Some(values.as_slice()),
            _ => None,
        })
    }
}

impl<'a> Antab<'a> {
    pub(super) fn read(text: &'a str) -> Result<Self, String> {
        let mut lines = text.lines();
        let mut header = String::new();
        let mut gains = Vec::new();
        let mut tsys = Vec::new();
        while let Some(line) = lines.next() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('!') {
                continue;
            }
            header.push_str(trimmed);
            header.push('\n');
            if !trimmed.ends_with('/') {
                continue;
            }
            let parsed = parse_header(&header)
                .map_err(|message| format!("error parsing ANTAB file: {message}"))?;
            if let Some((kind, _)) = parsed.0.first() {
                match kind.as_str() {
                    "GAIN" => gains.push(parsed),
                    "TSYS" => tsys.push((parsed, tsys_rows(&mut lines))),
                    _ => {}
                }
            }
            header.clear();
        }
        Ok(Self { gains, tsys })
    }
}

fn tsys_rows<'a>(lines: &mut std::str::Lines<'a>) -> Vec<&'a str> {
    let mut rows = Vec::new();
    for line in lines {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('!') {
            continue;
        }
        if let Some(last) = trimmed.strip_suffix('/') {
            if !last.trim().is_empty() {
                rows.push(last);
            }
            break;
        }
        rows.push(trimmed);
    }
    rows
}

#[derive(Debug)]
enum Token {
    Keyword(String),
    Value(String),
    Equal,
    Comma,
    End,
}

fn parse_header(text: &str) -> Result<Header, String> {
    let mut tokens = tokenize(text)?.into_iter().peekable();
    let mut fields = Vec::new();
    while let Some(token) = tokens.next() {
        let key = match token {
            Token::Keyword(key) => key,
            Token::End => return Ok(Header(fields)),
            _ => return Err("expected a header keyword".into()),
        };
        let explicit = matches!(tokens.peek(), Some(Token::Equal));
        if explicit {
            tokens.next();
        }
        let value = if explicit || matches!(tokens.peek(), Some(Token::Value(_))) {
            let mut values = Vec::new();
            loop {
                let word = match tokens.next() {
                    Some(Token::Keyword(word) | Token::Value(word)) => word,
                    _ => return Err(format!("expected a value for {key}")),
                };
                values.push(word);
                if !matches!(tokens.peek(), Some(Token::Comma)) {
                    break;
                }
                tokens.next();
            }
            Field::Values(values)
        } else {
            Field::Switch
        };
        fields.push((key, value));
    }
    Ok(Header(fields))
}

fn tokenize(text: &str) -> Result<Vec<Token>, String> {
    let mut input = text.chars().peekable();
    let mut tokens = Vec::new();
    while let Some(first) = input.next() {
        let mut word = first.to_string();
        let token = match first {
            '\'' | '"' => {
                word.clear();
                loop {
                    match input.next() {
                        Some(character) if character == first => break,
                        Some('\n') | None => return Err("unterminated quoted value".into()),
                        Some(character) => word.push(character),
                    }
                }
                Token::Value(word)
            }
            '!' => {
                input
                    .by_ref()
                    .take_while(|&character| character != '\n')
                    .count();
                continue;
            }
            '=' => Token::Equal,
            ',' => Token::Comma,
            '/' => Token::End,
            c if c.is_whitespace() => continue,
            _ => {
                while input.peek().is_some_and(|character| {
                    !character.is_whitespace() && !matches!(character, '=' | ',' | '/' | '!')
                }) {
                    word.push(input.next().expect("checked token"));
                }
                if word.len() <= 9
                    && !word.contains('.')
                    && word
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || "_()+-".contains(c))
                    && word.parse::<f64>().is_err()
                {
                    Token::Keyword(word.to_ascii_uppercase())
                } else {
                    Token::Value(word)
                }
            }
        };
        tokens.push(token);
    }
    Ok(tokens)
}
