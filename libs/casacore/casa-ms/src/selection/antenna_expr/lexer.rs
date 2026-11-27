// SPDX-License-Identifier: LGPL-3.0-or-later
//! The antenna expression scanner, a port of `ms/MSSel/MSAntennaGram.ll`.
//!
//! flex takes the longest match at each position and, between equally long
//! matches, the rule written first. [`Lexer::next_token`] measures every
//! rule's match and applies that choice, so for example `ea01` is a `NAME`
//! (an [`Kind::Identifier`]) although `PATTERN` matches it as long, `ea0*` is
//! a `PATTERN` (a [`Kind::QString`]), and a purely alphabetic word is a
//! [`Kind::Unit`]. Tokens borrow the expression text; scanning allocates
//! nothing.

/// Token kinds: the terminals of `MSAntennaGram.yy:45-64`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    /// `{INT}`: decimal digits.
    Int,
    /// `{FLOAT}`.
    Float,
    /// `{UNIT}`: letters only. The grammar also accepts it as a name.
    Unit,
    /// A quoted `{STRING}` or an unquoted `{PATTERN}`; both are patterns.
    QString,
    /// `{REGEX}` without `&`.
    Regex,
    /// `{REGEX}` containing `&`: a baseline regex.
    BlRegex,
    /// `{NAME}` or `{ESCNAME}`.
    Identifier,
    Semicolon,
    At,
    Ampersand,
    /// `~`, which the grammar calls `DASH`.
    Dash,
    Comma,
    Not,
    LParen,
    RParen,
    Lt,
    Le,
    Gt,
    Ge,
    /// Any other character: flex's `.` rule returns `YYERRCODE`, which the
    /// parser reports as a syntax error (`MSAntennaGram.ll:157`).
    Invalid,
    /// End of the expression.
    End,
}

/// One scanned token.
#[derive(Clone, Copy, Debug)]
pub(super) struct Token<'s> {
    pub(super) kind: Kind,
    /// The matched text, flex's `yytext`.
    pub(super) lexeme: &'s str,
    /// The token's string value (`$n`): the lexeme without its quotes,
    /// slashes or escaping backslash.
    pub(super) value: &'s str,
    /// For a regex written `^/.../`: the scanner overwrote the first
    /// character of its value with `^` (`MSAntennaGram.ll:81`), so the value
    /// the parser sees is `^` followed by [`value`](Self::value).
    pub(super) caret: bool,
}

impl Token<'_> {
    /// The string value as the C++ parser sees it, with a leading `^`
    /// stripped as negation (the convention of `MSAntennaIndex.cc:95-98`
    /// and `MSAntennaParse.cc:261-264`): `(negated, rest)`.
    pub(super) fn negation(&self) -> (bool, &str) {
        if self.caret {
            (true, self.value)
        } else if let Some(rest) = self.value.strip_prefix('^') {
            (true, rest)
        } else {
            (false, self.value)
        }
    }

    /// The string value as the C++ parser sees it, for messages.
    pub(super) fn shown(&self) -> String {
        if self.caret {
            format!("^{}", self.value)
        } else {
            self.value.to_string()
        }
    }
}

/// The scanner's own refusals, thrown as `MSSelectionAntennaError`
/// (`MSAntennaGram.ll:147-151`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum LexError {
    UnterminatedString,
    UnterminatedRegex,
}

impl LexError {
    pub(super) fn message(self) -> &'static str {
        match self {
            Self::UnterminatedString => "Unterminated string",
            Self::UnterminatedRegex => "Unterminated regex",
        }
    }
}

/// flex's rules, in the order written (`MSAntennaGram.ll:69-157`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Rule {
    String,
    Regex,
    Unit,
    Name,
    EscName,
    Pattern,
    Int,
    Float,
    Punctuation(Kind),
    White,
    Quote,
    Slash,
    Any,
}

/// The scanner state: the expression and the position counter
/// `msAntennaGramPosition()` the C++ advances for most tokens.
pub(super) struct Lexer<'s> {
    text: &'s str,
    offset: usize,
    position: usize,
}

impl<'s> Lexer<'s> {
    pub(super) fn new(text: &'s str) -> Self {
        Self {
            text,
            offset: 0,
            position: 0,
        }
    }

    /// `msAntennaGramPosition()`: the characters counted so far. Like the
    /// C++, strings, regexes and invalid characters do not advance it.
    pub(super) fn position(&self) -> usize {
        self.position
    }

    /// Scan the next token, skipping whitespace.
    pub(super) fn next_token(&mut self) -> Result<Token<'s>, LexError> {
        loop {
            let bytes = &self.text.as_bytes()[self.offset..];
            if bytes.is_empty() {
                return Ok(token(Kind::End, "", "", false));
            }
            let (rule, length) = longest_match(bytes);
            let start = self.offset;
            let lexeme = &self.text[start..start + length];
            self.offset += length;
            let counted = !matches!(
                rule,
                Rule::String | Rule::Regex | Rule::Any | Rule::Quote | Rule::Slash
            );
            if counted {
                self.position += length;
            }
            let scanned = match rule {
                Rule::White => continue,
                Rule::Quote => return Err(LexError::UnterminatedString),
                Rule::Slash => return Err(LexError::UnterminatedRegex),
                Rule::String => token(Kind::QString, lexeme, &lexeme[1..length - 1], false),
                Rule::Regex => regex_token(lexeme),
                Rule::Unit => token(Kind::Unit, lexeme, lexeme, false),
                Rule::Name => token(Kind::Identifier, lexeme, lexeme, false),
                Rule::EscName => token(Kind::Identifier, lexeme, &lexeme[1..], false),
                Rule::Pattern => token(Kind::QString, lexeme, lexeme, false),
                Rule::Int => token(Kind::Int, lexeme, lexeme, false),
                Rule::Float => token(Kind::Float, lexeme, lexeme, false),
                Rule::Punctuation(kind) => token(kind, lexeme, lexeme, false),
                Rule::Any => token(Kind::Invalid, lexeme, lexeme, false),
            };
            return Ok(scanned);
        }
    }
}

fn token<'s>(kind: Kind, lexeme: &'s str, value: &'s str, caret: bool) -> Token<'s> {
    Token {
        kind,
        lexeme,
        value,
        caret,
    }
}

/// `{REGEX}` (`MSAntennaGram.ll:77-87`): the text between the slashes; a
/// leading `^/` makes the value start with `^`; a value with `&` is a
/// baseline regex.
fn regex_token(lexeme: &str) -> Token<'_> {
    let caret = lexeme.starts_with('^');
    let value = &lexeme[1 + usize::from(caret)..lexeme.len() - 1];
    let kind = if value.contains('&') {
        Kind::BlRegex
    } else {
        Kind::Regex
    };
    token(kind, lexeme, value, caret)
}

/// The rule flex chooses at the start of `bytes` and its match length.
fn longest_match(bytes: &[u8]) -> (Rule, usize) {
    let candidates = [
        (Rule::String, string_length(bytes)),
        (Rule::Regex, regex_length(bytes)),
        (Rule::Unit, run(bytes, 0, u8::is_ascii_alphabetic)),
        (Rule::Name, name_length(bytes)),
        (Rule::EscName, escaped_name_length(bytes)),
        (Rule::Pattern, pattern_length(bytes)),
        (Rule::Int, run(bytes, 0, u8::is_ascii_digit)),
        (Rule::Float, float_length(bytes)),
        punctuation(bytes),
        (
            Rule::White,
            run(bytes, 0, |b| matches!(b, b' ' | b'\t' | b'\n')),
        ),
        (Rule::Quote, usize::from(matches!(bytes[0], b'"' | b'\''))),
        (Rule::Slash, usize::from(bytes[0] == b'/')),
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

/// `NAMECHAR`: `[-+a-zA-Z0-9_.]`.
fn is_name_char(byte: &u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'+' | b'_' | b'.')
}

/// `NAMENNUM`: `[a-zA-Z_]`.
fn is_name_start(byte: &u8) -> bool {
    byte.is_ascii_alphabetic() || *byte == b'_'
}

/// `PATTCHAR`: `[][*?^]`.
fn is_pattern_char(byte: &u8) -> bool {
    matches!(byte, b'[' | b']' | b'*' | b'?' | b'^')
}

/// `({QSTRING}|{ASTRING})+`: one or more complete quoted segments.
fn string_length(bytes: &[u8]) -> usize {
    let mut end = 0;
    while let Some(&quote) = bytes.get(end).filter(|b| matches!(b, b'"' | b'\'')) {
        let body = run(bytes, end + 1, |b| *b != quote && *b != b'\n');
        if bytes.get(end + 1 + body) != Some(&quote) {
            break;
        }
        end += body + 2;
    }
    end
}

/// `"^"?"/"[^/\n]*"/"`.
fn regex_length(bytes: &[u8]) -> usize {
    let start = usize::from(bytes[0] == b'^');
    if bytes.get(start) != Some(&b'/') {
        return 0;
    }
    let body = run(bytes, start + 1, |b| *b != b'/' && *b != b'\n');
    if bytes.get(start + 1 + body) == Some(&b'/') {
        start + body + 2
    } else {
        0
    }
}

/// `{NAME1}|{NAME2}`: a name start then name characters, or a run of name
/// characters and colons that holds a colon.
fn name_length(bytes: &[u8]) -> usize {
    let first = if is_name_start(&bytes[0]) {
        1 + run(bytes, 1, is_name_char)
    } else {
        0
    };
    first.max(colon_run(bytes, is_name_char))
}

/// `{PATT1}|{PATT2}`: as [`name_length`] with the pattern characters added.
fn pattern_length(bytes: &[u8]) -> usize {
    let in_pattern = |b: &u8| is_name_char(b) || is_pattern_char(b);
    let first = if is_name_start(&bytes[0]) || is_pattern_char(&bytes[0]) {
        1 + run(bytes, 1, in_pattern)
    } else {
        0
    };
    first.max(colon_run(bytes, in_pattern))
}

/// `X*":"(X|":")*`: the run of `X` and colons, if it contains a colon.
fn colon_run(bytes: &[u8], accept: impl Fn(&u8) -> bool) -> usize {
    let length = run(bytes, 0, |b| *b == b':' || accept(b));
    if bytes[..length].contains(&b':') {
        length
    } else {
        0
    }
}

/// `\\[^ \t\n]+`.
fn escaped_name_length(bytes: &[u8]) -> usize {
    if bytes[0] != b'\\' {
        return 0;
    }
    match run(bytes, 1, |b| !matches!(b, b' ' | b'\t' | b'\n')) {
        0 => 0,
        rest => rest + 1,
    }
}

/// `{INT}{EXP}|{INT}"."{DIGIT}*({EXP})?|{DIGIT}*"."{INT}({EXP})?`.
fn float_length(bytes: &[u8]) -> usize {
    let integer = run(bytes, 0, u8::is_ascii_digit);
    let mut best = 0;
    if integer > 0 {
        best = exponent_length(bytes, integer).map_or(0, |e| integer + e);
    }
    if bytes.get(integer) == Some(&b'.') {
        let fraction = run(bytes, integer + 1, u8::is_ascii_digit);
        if integer > 0 || fraction > 0 {
            let mantissa = integer + 1 + fraction;
            best = best.max(mantissa + exponent_length(bytes, mantissa).unwrap_or(0));
        }
    }
    best
}

/// `[DdEe][+-]?{INT}` at `start`.
fn exponent_length(bytes: &[u8], start: usize) -> Option<usize> {
    if !matches!(bytes.get(start), Some(b'D' | b'd' | b'E' | b'e')) {
        return None;
    }
    let sign = usize::from(matches!(bytes.get(start + 1), Some(b'+' | b'-')));
    match run(bytes, start + 1 + sign, u8::is_ascii_digit) {
        0 => None,
        digits => Some(1 + sign + digits),
    }
}

/// The single- and two-character operator rules (`MSAntennaGram.ll:132-143`).
fn punctuation(bytes: &[u8]) -> (Rule, usize) {
    let second_is_equals = bytes.get(1) == Some(&b'=');
    let (kind, length) = match bytes[0] {
        b';' => (Kind::Semicolon, 1),
        b'@' => (Kind::At, 1),
        b'&' => (Kind::Ampersand, 1),
        b'~' => (Kind::Dash, 1),
        b',' => (Kind::Comma, 1),
        b'!' => (Kind::Not, 1),
        b'(' => (Kind::LParen, 1),
        b')' => (Kind::RParen, 1),
        b'<' if second_is_equals => (Kind::Le, 2),
        b'<' => (Kind::Lt, 1),
        b'>' if second_is_equals => (Kind::Ge, 2),
        b'>' => (Kind::Gt, 1),
        _ => return (Rule::Any, 0),
    };
    (Rule::Punctuation(kind), length)
}

/// flex's `.`: one byte other than a newline. A multi-byte character is
/// taken whole so the lexeme stays valid UTF-8; the parser fails on it
/// either way.
fn any_length(bytes: &[u8]) -> usize {
    if bytes[0] == b'\n' {
        return 0;
    }
    1 + bytes[1..]
        .iter()
        .take_while(|byte| (**byte & 0xC0) == 0x80)
        .count()
}
