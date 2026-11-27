// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[derive(Debug, Clone, PartialEq)]
pub(super) enum Token {
    Number(f64),
    Ident(String),
    QuotedPath(String),
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Caret,
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    And,
    Or,
    Bang,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Colon,
    Comma,
    Eof,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::Number(n) => write!(f, "{n}"),
            Token::Ident(s) => write!(f, "{s}"),
            Token::QuotedPath(s) => write!(f, "'{s}'"),
            Token::Plus => write!(f, "+"),
            Token::Minus => write!(f, "-"),
            Token::Star => write!(f, "*"),
            Token::Slash => write!(f, "/"),
            Token::Percent => write!(f, "%"),
            Token::Caret => write!(f, "^"),
            Token::Eq => write!(f, "=="),
            Token::Ne => write!(f, "!="),
            Token::Gt => write!(f, ">"),
            Token::Ge => write!(f, ">="),
            Token::Lt => write!(f, "<"),
            Token::Le => write!(f, "<="),
            Token::And => write!(f, "&&"),
            Token::Or => write!(f, "||"),
            Token::Bang => write!(f, "!"),
            Token::LParen => write!(f, "("),
            Token::RParen => write!(f, ")"),
            Token::LBracket => write!(f, "["),
            Token::RBracket => write!(f, "]"),
            Token::Colon => write!(f, ":"),
            Token::Comma => write!(f, ","),
            Token::Eof => write!(f, "<eof>"),
        }
    }
}

// ---------------------------------------------------------------------------
// Tokenizer
// ---------------------------------------------------------------------------

pub(super) struct Lexer<'s> {
    pub(super) src: &'s str,
    pub(super) pos: usize,
}

impl<'s> Lexer<'s> {
    pub(super) fn new(src: &'s str) -> Self {
        Self { src, pos: 0 }
    }

    fn peek_char(&self) -> Option<char> {
        self.src[self.pos..].chars().next()
    }

    fn advance_char(&mut self) -> Option<char> {
        let ch = self.peek_char()?;
        self.pos += ch.len_utf8();
        Some(ch)
    }

    fn skip_whitespace(&mut self) {
        while let Some(ch) = self.peek_char() {
            if ch.is_ascii_whitespace() {
                self.advance_char();
            } else {
                break;
            }
        }
    }

    pub(super) fn next_token(&mut self) -> Result<(Token, usize), ExprParseError> {
        self.skip_whitespace();
        let start = self.pos;

        let ch = match self.peek_char() {
            None => return Ok((Token::Eof, start)),
            Some(c) => c,
        };

        // Single-char operators
        match ch {
            '+' => {
                self.advance_char();
                return Ok((Token::Plus, start));
            }
            '*' => {
                self.advance_char();
                return Ok((Token::Star, start));
            }
            '/' => {
                self.advance_char();
                return Ok((Token::Slash, start));
            }
            '%' => {
                self.advance_char();
                return Ok((Token::Percent, start));
            }
            '^' => {
                self.advance_char();
                return Ok((Token::Caret, start));
            }
            '(' => {
                self.advance_char();
                return Ok((Token::LParen, start));
            }
            ')' => {
                self.advance_char();
                return Ok((Token::RParen, start));
            }
            '[' => {
                self.advance_char();
                return Ok((Token::LBracket, start));
            }
            ']' => {
                self.advance_char();
                return Ok((Token::RBracket, start));
            }
            ':' => {
                self.advance_char();
                return Ok((Token::Colon, start));
            }
            ',' => {
                self.advance_char();
                return Ok((Token::Comma, start));
            }
            _ => {}
        }

        // Two-char operators and single-char fallbacks
        match ch {
            '-' => {
                self.advance_char();
                return Ok((Token::Minus, start));
            }
            '=' => {
                self.advance_char();
                if self.peek_char() == Some('=') {
                    self.advance_char();
                    return Ok((Token::Eq, start));
                }
                return Err(ExprParseError {
                    message: "expected '==' but found lone '='".into(),
                    position: start,
                });
            }
            '!' => {
                self.advance_char();
                if self.peek_char() == Some('=') {
                    self.advance_char();
                    return Ok((Token::Ne, start));
                }
                return Ok((Token::Bang, start));
            }
            '>' => {
                self.advance_char();
                if self.peek_char() == Some('=') {
                    self.advance_char();
                    return Ok((Token::Ge, start));
                }
                return Ok((Token::Gt, start));
            }
            '<' => {
                self.advance_char();
                if self.peek_char() == Some('=') {
                    self.advance_char();
                    return Ok((Token::Le, start));
                }
                return Ok((Token::Lt, start));
            }
            '&' => {
                self.advance_char();
                if self.peek_char() == Some('&') {
                    self.advance_char();
                    return Ok((Token::And, start));
                }
                return Err(ExprParseError {
                    message: "expected '&&' but found lone '&'".into(),
                    position: start,
                });
            }
            '|' => {
                self.advance_char();
                if self.peek_char() == Some('|') {
                    self.advance_char();
                    return Ok((Token::Or, start));
                }
                return Err(ExprParseError {
                    message: "expected '||' but found lone '|'".into(),
                    position: start,
                });
            }
            _ => {}
        }

        // Quoted path
        if ch == '\'' || ch == '"' {
            return self.lex_quoted_path(ch, start);
        }

        // Number: starts with digit or dot-followed-by-digit
        if ch.is_ascii_digit()
            || (ch == '.'
                && self.pos + 1 < self.src.len()
                && self.src.as_bytes()[self.pos + 1].is_ascii_digit())
        {
            return self.lex_number(start);
        }

        // Identifier or bare path
        if ch.is_ascii_alphabetic() || ch == '_' {
            return self.lex_ident(start);
        }

        Err(ExprParseError {
            message: format!("unexpected character '{ch}'"),
            position: start,
        })
    }

    fn lex_quoted_path(
        &mut self,
        quote: char,
        start: usize,
    ) -> Result<(Token, usize), ExprParseError> {
        self.advance_char(); // consume opening quote
        let content_start = self.pos;
        loop {
            match self.advance_char() {
                None => {
                    return Err(ExprParseError {
                        message: format!("unterminated quoted string starting with {quote}"),
                        position: start,
                    });
                }
                Some(c) if c == quote => {
                    let content = self.src[content_start..self.pos - quote.len_utf8()].to_string();
                    return Ok((Token::QuotedPath(content), start));
                }
                Some(_) => {}
            }
        }
    }

    fn lex_number(&mut self, start: usize) -> Result<(Token, usize), ExprParseError> {
        // integer part
        while let Some(c) = self.peek_char() {
            if c.is_ascii_digit() {
                self.advance_char();
            } else {
                break;
            }
        }
        // fractional part
        if self.peek_char() == Some('.') {
            self.advance_char();
            while let Some(c) = self.peek_char() {
                if c.is_ascii_digit() {
                    self.advance_char();
                } else {
                    break;
                }
            }
        }
        // exponent (e/E/d/D)
        if let Some(c) = self.peek_char()
            && (c == 'e' || c == 'E' || c == 'd' || c == 'D')
        {
            self.advance_char();
            if let Some(s) = self.peek_char()
                && (s == '+' || s == '-')
            {
                self.advance_char();
            }
            while let Some(d) = self.peek_char() {
                if d.is_ascii_digit() {
                    self.advance_char();
                } else {
                    break;
                }
            }
        }
        let text = &self.src[start..self.pos];
        // Replace 'd'/'D' exponent marker with 'e' for Rust parsing
        let normalized = text.replace(['d', 'D'], "e");
        let value: f64 = normalized.parse().map_err(|_| ExprParseError {
            message: format!("invalid number literal: {text}"),
            position: start,
        })?;
        Ok((Token::Number(value), start))
    }

    fn lex_ident(&mut self, start: usize) -> Result<(Token, usize), ExprParseError> {
        while let Some(c) = self.peek_char() {
            if c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '/' || c == '~' {
                self.advance_char();
            } else {
                break;
            }
        }
        // A named image mask is a single lattice operand. Numeric ranges
        // in index arrays still lex as a number, a colon and a number.
        if self.peek_char() == Some(':')
            && self.src[self.pos + 1..]
                .starts_with(|character: char| character.is_ascii_alphabetic() || character == '_')
        {
            self.advance_char();
            while self.peek_char().is_some_and(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '_' | '.')
            }) {
                self.advance_char();
            }
        }
        let text = self.src[start..self.pos].to_string();
        Ok((Token::Ident(text), start))
    }
}

/// Returns the image operands in a lattice expression, in first-use order.
///
/// Both bare and quoted image names are included. Function names and CASA
/// Boolean literals are excluded.
///
/// # Errors
/// Returns a tokenizer error if the expression is lexically invalid.
pub fn referenced_images(expression: &str) -> Result<Vec<String>, ExprParseError> {
    let mut lexer = Lexer::new(expression);
    let mut current = lexer.next_token()?.0;
    let mut names = Vec::new();
    while current != Token::Eof {
        let (next, position) = lexer.next_token()?;
        if matches!(current, Token::QuotedPath(_)) && next == Token::Colon {
            let selector = expression[position + 1..]
                .split(|character: char| {
                    !character.is_ascii_alphanumeric() && !matches!(character, '_' | '.')
                })
                .next()
                .unwrap_or_default();
            return Err(ExprParseError {
                message: format!("Parse error at or near ':{selector}'"),
                position,
            });
        }
        let name = match current {
            Token::QuotedPath(name) => Some(name),
            Token::Ident(name) if !matches!(name.as_str(), "T" | "F") && next != Token::LParen => {
                Some(name)
            }
            _ => None,
        };
        if let Some(name) = name
            && !names.contains(&name)
        {
            names.push(name);
        }
        current = next;
    }
    Ok(names)
}
