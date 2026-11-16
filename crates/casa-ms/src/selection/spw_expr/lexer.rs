// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `spw` expression scanner, a port of `ms/MSSel/MSSpwGram.ll`.
//!
//! flex takes the longest match at each position and, between equally long
//! matches, the rule written first; [`Lexer::next_token`] measures every
//! rule's match and applies that choice. So `spw1` is an identifier (a name)
//! although the pattern rule matches it as long, `spw*` is a pattern, `e5`
//! and `+2` are numbers, and `MHz` and `km/s` are units however a name rule
//! would read them. Tokens borrow the expression text except a quoted string
//! or regex, whose value drops the characters flex's default rule echoes.

use std::borrow::Cow;

/// Token kinds: the terminals `MSSpwGram.ll` returns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    /// `{FNUMBER}`: `[+-]?[0-9]*\.?[0-9]*([DdEe][+-]?[0-9]+)?`.
    Number,
    /// `{UNIT}`: a frequency (`[kMGT]?Hz`, any case) or velocity unit.
    Unit,
    /// `~`, which the grammar calls `DASH`.
    Dash,
    Comma,
    Lt,
    Gt,
    /// `<>`.
    GtNLt,
    /// `&`: scanned, but no rule takes it.
    Ampersand,
    Semicolon,
    Colon,
    Caret,
    LParen,
    RParen,
    /// `{IDENTIFIER}`: an exact name.
    Identifier,
    /// A quoted string or a bare `{SIDENTIFIER}` (a name holding `*` or
    /// `?`): a pattern. The grammar calls both `QSTRING`.
    QString,
    /// `/.../`, which the grammar also matches as a pattern.
    Regex,
    /// Any other ASCII character: flex's `.` rule returns it as a token
    /// number the parser does not know, a syntax error.
    Invalid,
    /// The end of the expression.
    End,
}

/// One scanned token.
#[derive(Clone, Debug)]
pub(super) struct Token<'s> {
    pub(super) kind: Kind,
    /// The matched text, flex's `yytext`, for messages.
    pub(super) lexeme: &'s str,
    /// The string value of an identifier, string, pattern or regex.
    pub(super) value: Cow<'s, str>,
}

/// flex's rules in the order written (`MSSpwGram.ll:84-175`), for the
/// tie-break between equally long matches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Rule {
    Number,
    Unit,
    Punctuation(Kind),
    Identifier,
    Pattern,
    White,
    Any,
}

/// The scanner over one expression.
pub(super) struct Lexer<'s> {
    text: &'s str,
    offset: usize,
}

impl<'s> Lexer<'s> {
    /// Scan `text` as the C++ reads it: a C string, so up to its first NUL.
    pub(super) fn new(text: &'s str) -> Self {
        let text = text.split('\0').next().unwrap_or_default();
        Self { text, offset: 0 }
    }

    /// Scan the next token, skipping whitespace.
    pub(super) fn next_token(&mut self) -> Token<'s> {
        loop {
            let bytes = &self.text.as_bytes()[self.offset..];
            let Some(&first) = bytes.first() else {
                return self.end();
            };
            // `{QUOTE}` and `{RQUOTE}` (`.ll:84-117`) win over `.`: no other
            // rule matches `"` or `/`, and both are one character long.
            if first == b'"' || first == b'/' {
                return self.delimited(first);
            }
            let (rule, length) = longest_match(bytes);
            let start = self.offset;
            let lexeme = &self.text[start..start + length];
            self.offset += length;
            let kind = match rule {
                Rule::White => continue,
                Rule::Number => Kind::Number,
                Rule::Unit => Kind::Unit,
                Rule::Punctuation(kind) => kind,
                Rule::Identifier => Kind::Identifier,
                Rule::Pattern => Kind::QString,
                // `.` returns `MSSpwGramtext[0]`, a signed `char`: a byte
                // from 0x80 up is a negative token number, which bison
                // takes as the end of input (`yychar <= YYEOF`).
                Rule::Any if first >= 0x80 => {
                    self.offset = self.text.len();
                    return self.end();
                }
                Rule::Any => Kind::Invalid,
            };
            return Token {
                kind,
                lexeme,
                value: Cow::Borrowed(lexeme),
            };
        }
    }

    fn end(&self) -> Token<'s> {
        Token {
            kind: Kind::End,
            lexeme: "",
            value: Cow::Borrowed(""),
        }
    }

    /// A quoted string (`"..."`) or regex (`/.../`) in flex's exclusive
    /// start states `QS` and `RS`. Inside, every character but a backslash
    /// and a newline is kept; those two match no rule of the state, so
    /// flex's default rule echoes them to standard output and the value
    /// loses them. An unterminated string or regex reaches the end of input
    /// inside the state, which ends the expression without a token.
    fn delimited(&mut self, delimiter: u8) -> Token<'s> {
        let start = self.offset;
        let body_start = start + 1;
        let bytes = self.text.as_bytes();
        let Some(close) = bytes[body_start..]
            .iter()
            .position(|&byte| byte == delimiter)
        else {
            self.offset = self.text.len();
            return self.end();
        };
        let body = &self.text[body_start..body_start + close];
        self.offset = body_start + close + 1;
        let value = if body.contains(['\\', '\n']) {
            Cow::Owned(body.replace(['\\', '\n'], ""))
        } else {
            Cow::Borrowed(body)
        };
        Token {
            kind: if delimiter == b'"' {
                Kind::QString
            } else {
                Kind::Regex
            },
            lexeme: &self.text[start..self.offset],
            value,
        }
    }
}

/// The rule flex chooses at the start of `bytes` and its match length.
fn longest_match(bytes: &[u8]) -> (Rule, usize) {
    let candidates = [
        (Rule::Number, number_length(bytes)),
        (Rule::Unit, unit_length(bytes)),
        punctuation(bytes),
        (Rule::Identifier, identifier_length(bytes)),
        (Rule::Pattern, pattern_length(bytes)),
        (
            Rule::White,
            run(bytes, 0, |b| matches!(b, b' ' | b'\t' | b'\n')),
        ),
        (Rule::Any, any_length(bytes)),
    ];
    // The first of the longest: flex's tie-break by rule order.
    candidates
        .into_iter()
        .fold((Rule::Any, 0), |best, candidate| {
            if candidate.1 > best.1 {
                candidate
            } else {
                best
            }
        })
}

/// The length of the run of bytes satisfying `accept` from `start`.
fn run(bytes: &[u8], start: usize, accept: impl Fn(&u8) -> bool) -> usize {
    bytes.get(start..).map_or(0, |rest| {
        rest.iter().take_while(|byte| accept(byte)).count()
    })
}

/// `{FNUMBER}`: `{SIGN}?{NUMBER}?{EXP}?` with `NUMBER` `[0-9]*\.?[0-9]*`
/// (`.ll:41-48`). Every part is optional, so the greedy reading is the
/// longest; it may be a lone sign or point, or empty.
fn number_length(bytes: &[u8]) -> usize {
    let mut end = usize::from(matches!(bytes[0], b'+' | b'-'));
    end += run(bytes, end, u8::is_ascii_digit);
    if bytes.get(end) == Some(&b'.') {
        end += 1;
        end += run(bytes, end, u8::is_ascii_digit);
    }
    if matches!(bytes.get(end), Some(b'D' | b'd' | b'E' | b'e')) {
        let sign = usize::from(matches!(bytes.get(end + 1), Some(b'+' | b'-')));
        let digits = run(bytes, end + 1 + sign, u8::is_ascii_digit);
        if digits > 0 {
            end += 1 + sign + digits;
        }
    }
    end
}

/// `{UNIT}`: `{FREQ}|{VELOCITY}` (`.ll:50-59`), where `FREQ` is
/// `[KkMmGgTt]?[Hh][Zz]` and `VELOCITY` `([Kk]|[Mm])?m/([Ss]|[Ss][Ee][Cc])`
/// with a lower-case metre.
fn unit_length(bytes: &[u8]) -> usize {
    let at = |index: usize, accept: &[u8]| bytes.get(index).is_some_and(|b| accept.contains(b));
    let hertz = |start: usize| at(start, b"Hh") && at(start + 1, b"Zz");
    let mut best = 0;
    if hertz(0) {
        best = 2;
    }
    if at(0, b"KkMmGgTt") && hertz(1) {
        best = 3;
    }
    let velocity = |start: usize| {
        if !(at(start, b"m") && at(start + 1, b"/") && at(start + 2, b"Ss")) {
            return 0;
        }
        if at(start + 3, b"Ee") && at(start + 4, b"Cc") {
            start + 5
        } else {
            start + 3
        }
    };
    best = best.max(velocity(0));
    if at(0, b"KkMm") {
        best = best.max(velocity(1));
    }
    best
}

/// The operator rules (`.ll:130-155`); `<>` is longer than `<`.
fn punctuation(bytes: &[u8]) -> (Rule, usize) {
    let (kind, length) = match bytes[0] {
        b'~' => (Kind::Dash, 1),
        b',' => (Kind::Comma, 1),
        b'<' if bytes.get(1) == Some(&b'>') => (Kind::GtNLt, 2),
        b'<' => (Kind::Lt, 1),
        b'>' => (Kind::Gt, 1),
        b'&' => (Kind::Ampersand, 1),
        b';' => (Kind::Semicolon, 1),
        b':' => (Kind::Colon, 1),
        b'^' => (Kind::Caret, 1),
        b'(' => (Kind::LParen, 1),
        b')' => (Kind::RParen, 1),
        _ => return (Rule::Any, 0),
    };
    (Rule::Punctuation(kind), length)
}

/// `{IDENTIFIER}`, `({NAME}+)` with `NAME`
/// `[a-zA-Z_'{''}''+''-'#]+[a-zA-Z0-9_{}#]*` (`.ll:69-72`): in flex's
/// class syntax `''-''` is the range from `'` to `'`, so `-` is not a name
/// character. A name starts with a letter, `_`, `'`, `{`, `}`, `+` or `#`
/// and continues with those or digits.
fn identifier_length(bytes: &[u8]) -> usize {
    let start =
        |b: &u8| b.is_ascii_alphabetic() || matches!(b, b'_' | b'\'' | b'{' | b'}' | b'+' | b'#');
    if !start(&bytes[0]) {
        return 0;
    }
    1 + run(bytes, 1, |b| start(b) || b.is_ascii_digit())
}

/// `{SIDENTIFIER}` (`.ll:73`): as a name, with `*` and `?` allowed
/// anywhere; returned as `QSTRING`, a pattern.
fn pattern_length(bytes: &[u8]) -> usize {
    let start = |b: &u8| {
        b.is_ascii_alphabetic()
            || matches!(b, b'_' | b'*' | b'?' | b'{' | b'}' | b'\'' | b'+' | b'#')
    };
    if !start(&bytes[0]) {
        return 0;
    }
    1 + run(bytes, 1, |b| start(b) || b.is_ascii_digit())
}

/// flex's `.`: one byte other than a newline. A multi-byte character is
/// taken whole so the lexeme stays valid UTF-8.
fn any_length(bytes: &[u8]) -> usize {
    if bytes[0] == b'\n' {
        return 0;
    }
    1 + bytes[1..]
        .iter()
        .take_while(|byte| (**byte & 0xC0) == 0x80)
        .count()
}
