// SPDX-License-Identifier: LGPL-3.0-or-later
//! casacore `Regex` as antenna and spectral-window selection use it.
//!
//! Both match names with `String::matches(Regex)`, a full
//! match (`casa/BasicSL/String.cc:367-372`, `std::regex_match`). The regex
//! text first passes through `Regex::fromPattern` when it came from a
//! shell-style pattern (`casa/Utilities/Regex.cc:228-367`), and always through
//! `Regex::toEcma` (`Regex.cc:146-226`, applied by the `Regex(const String&)`
//! constructor, `Regex.cc:41-58`) before `std::regex` compiles it with the
//! ECMAScript grammar. Both conversions are ported byte for byte.
//!
//! The ECMAScript result is then translated to the `regex` crate's syntax
//! and matched over bytes with Unicode disabled, as `std::regex` over
//! `char` matches bytes. ECMAScript features the `regex` crate lacks
//! (backreferences, lookahead) are refused rather than approximated.

use regex::bytes::{Regex, RegexBuilder};

/// A compiled casacore regex that answers `String::matches`: whether the
/// whole of a string matches.
#[derive(Debug)]
pub(in crate::selection) struct FullMatch(Regex);

impl FullMatch {
    /// Compile casacore regex text: what `Regex(const String&)` receives,
    /// before its `toEcma` conversion.
    ///
    /// The `Err` text is the reason the expression cannot be compiled, as
    /// casacore's `AipsError("Error in regex ...")` (`Regex.cc:55-57`) or an
    /// ECMAScript feature this port does not support.
    pub(super) fn compile(casacore_regex: &str) -> Result<Self, String> {
        let ecma = to_ecma(casacore_regex.as_bytes());
        let body = ecma_to_rust(&ecma)
            .map_err(|reason| format!("Error in regex {casacore_regex}: {reason}"))?;
        RegexBuilder::new(&format!(r"\A(?:{body})\z"))
            .unicode(false)
            .build()
            .map(Self)
            .map_err(|error| format!("Error in regex {casacore_regex}: {error}"))
    }

    /// Compile a shell-style pattern as `Regex::fromPattern` does, then as
    /// [`compile`](Self::compile).
    pub(in crate::selection) fn compile_pattern(pattern: &str) -> Result<Self, String> {
        let regex = from_pattern(pattern.as_bytes());
        Self::compile(&String::from_utf8_lossy(&regex))
    }

    /// Whether all of `text` matches (`String::matches(const Regex&, 0)`).
    pub(in crate::selection) fn matches(&self, text: &str) -> bool {
        self.0.is_match(text.as_bytes())
    }
}

/// `Regex::fromPattern` (`casa/Utilities/Regex.cc:228-367`): a shell-style
/// pattern (`*`, `?`, `[...]`, `{a,b}`) as casacore regex text.
pub(super) fn from_pattern(pattern: &[u8]) -> Vec<u8> {
    #[derive(PartialEq)]
    enum State {
        Stream,
        BracketOpen,
        EscapeChar,
    }
    let mut brace_count = 0usize;
    let mut in_bracket_count: i32 = -1;
    let mut skip_char = false;
    let mut char_class = false;
    let mut empty_substrings: Vec<u32> = Vec::new();
    let mut result = Vec::with_capacity(3 * pattern.len());
    let mut state = State::Stream;
    let mut i = 0;
    while i < pattern.len() {
        let mut c = pattern[i];
        match state {
            State::Stream => match c {
                b'^' | b'$' | b'(' | b')' | b'.' | b'+' | b'|' => result.push(b'\\'),
                b'{' => {
                    c = b'(';
                    brace_count += 1;
                    empty_substrings.push(0);
                }
                b',' if brace_count > 0 => {
                    c = b'|';
                    if matches!(pattern[i - 1], b'{' | b',') {
                        empty_substrings[brace_count - 1] += 1;
                        skip_char = true;
                    }
                }
                b'}' => {
                    if brace_count > 0 {
                        brace_count -= 1;
                        c = b')';
                        if empty_substrings.pop().is_some_and(|empty| empty > 0) {
                            result.push(c);
                            c = b'?';
                        }
                    } else {
                        result.push(b'\\');
                    }
                }
                b'[' => {
                    state = State::BracketOpen;
                    char_class = false;
                    in_bracket_count = -1;
                }
                b'*' => result.push(b'.'),
                b'?' => c = b'.',
                b'\\' => state = State::EscapeChar,
                _ => {}
            },
            State::BracketOpen => {
                if c == b']' && in_bracket_count > 0 {
                    state = State::Stream;
                } else if matches!(c, b'!' | b'^') && in_bracket_count < 0 {
                    c = b'^';
                } else {
                    in_bracket_count = in_bracket_count.max(0);
                    if c == b'[' && pattern.get(i + 1) == Some(&b':') {
                        result.push(c);
                        i += 1;
                        c = pattern[i];
                        char_class = true;
                    } else if char_class && c == b':' && pattern.get(i + 1) == Some(&b']') {
                        result.push(c);
                        i += 1;
                        c = pattern[i];
                        char_class = false;
                    }
                }
                in_bracket_count += 1;
            }
            State::EscapeChar => {
                // An escaped comma needs no backslash.
                if c != b',' {
                    result.push(b'\\');
                }
                state = State::Stream;
            }
        }
        if !skip_char && state != State::EscapeChar {
            result.push(c);
        }
        skip_char = false;
        i += 1;
    }
    if state == State::EscapeChar {
        result.push(b'\\');
    }
    result
}

/// `Regex::toEcma` (`casa/Utilities/Regex.cc:146-226`): casacore (POSIX
/// flavoured) regex text as ECMAScript, escaping brackets that ECMAScript
/// would read differently.
pub(super) fn to_ecma(rx: &[u8]) -> Vec<u8> {
    let mut in_bracket_count: i32 = -1;
    let mut in_bracket = false;
    let mut char_class = false;
    let mut escaped = false;
    let mut result = Vec::with_capacity(rx.len());
    let mut i = 0;
    while i < rx.len() {
        let mut c = rx[i];
        if escaped {
            escaped = false;
            // A two-digit backreference keeps its second digit literal.
            if (b'1'..=b'9').contains(&c) && rx.get(i + 1).is_some_and(u8::is_ascii_digit) {
                result.extend_from_slice(&[c, b'[', rx[i + 1]]);
                c = b']';
                i += 1;
            }
        } else if c == b'\\' {
            escaped = true;
            in_bracket_count = 1;
        } else if !in_bracket {
            if c == b'[' {
                in_bracket = true;
                char_class = false;
                in_bracket_count = -1;
            } else if c == b']' {
                result.push(b'\\');
            }
        } else if c == b']' {
            if in_bracket_count > 0 {
                in_bracket = false;
            } else {
                result.push(b'\\');
                in_bracket_count = 0;
            }
        } else if c != b'^' {
            in_bracket_count = 0;
            if c == b'[' {
                if rx.get(i + 1) == Some(&b':') {
                    result.push(c);
                    i += 1;
                    c = rx[i];
                    char_class = true;
                } else {
                    result.push(b'\\');
                }
            } else if char_class && c == b':' && rx.get(i + 1) == Some(&b']') {
                result.push(c);
                i += 1;
                c = rx[i];
                char_class = false;
            }
        }
        in_bracket_count += 1;
        result.push(c);
        i += 1;
    }
    result
}

/// Translate ECMAScript regex text (bytes) to the `regex` crate's syntax
/// with the same meaning over bytes.
///
/// Every literal byte is written as `\xHH`, so no character is read as a
/// `regex`-crate operator ECMAScript does not have (`&&`, `--`, `~~` in a
/// class, `\<` and `\>` assertions). ECMAScript `.` excludes `\n` and `\r`.
fn ecma_to_rust(ecma: &[u8]) -> Result<String, &'static str> {
    let mut out = String::with_capacity(4 * ecma.len());
    let mut i = 0;
    while i < ecma.len() {
        let c = ecma[i];
        i += 1;
        match c {
            b'\\' => i = translate_escape(ecma, i, false, &mut out)?,
            b'[' => i = translate_class(ecma, i, &mut out)?,
            b'.' => out.push_str(r"[^\n\r]"),
            b'(' => {
                if ecma[i..].starts_with(b"?:") {
                    out.push_str("(?:");
                    i += 2;
                } else if ecma.get(i) == Some(&b'?') {
                    return Err("lookahead groups are not supported");
                } else {
                    out.push('(');
                }
            }
            b')' | b'|' | b'*' | b'+' | b'?' | b'{' | b'}' | b'^' | b'$' => out.push(c as char),
            b',' | b'0'..=b'9' if is_inside_repetition(&out) => out.push(c as char),
            _ => push_byte(&mut out, c),
        }
    }
    Ok(out)
}

/// Whether the translation so far ends inside an open `{n,m}` repetition,
/// whose digits and comma must stay bare.
fn is_inside_repetition(out: &str) -> bool {
    let bytes = out.as_bytes();
    let mut index = bytes.len();
    while index > 0 && matches!(bytes[index - 1], b'0'..=b'9' | b',') {
        index -= 1;
    }
    // `\{` was escaped as `\x7B`, so a bare `{` opens a repetition.
    index > 0 && bytes[index - 1] == b'{'
}

/// Translate the ECMAScript escape whose character is at `ecma[i]` (the
/// backslash already consumed); returns the index after it.
fn translate_escape(
    ecma: &[u8],
    i: usize,
    in_class: bool,
    out: &mut String,
) -> Result<usize, &'static str> {
    let Some(&c) = ecma.get(i) else {
        return Err("trailing backslash");
    };
    match c {
        b'd' | b'D' | b'w' | b'W' | b's' | b'S' => {
            out.push('\\');
            out.push(c as char);
        }
        b'b' | b'B' if !in_class => {
            out.push('\\');
            out.push(c as char);
        }
        b'b' => push_byte(out, 0x08),
        b'n' => push_byte(out, b'\n'),
        b't' => push_byte(out, b'\t'),
        b'r' => push_byte(out, b'\r'),
        b'f' => push_byte(out, 0x0c),
        b'v' => push_byte(out, 0x0b),
        b'0' if !ecma.get(i + 1).is_some_and(u8::is_ascii_digit) => push_byte(out, 0),
        b'1'..=b'9' | b'0' => return Err("backreferences are not supported"),
        b'x' => {
            let value = hex_value(ecma.get(i + 1..i + 3)).ok_or("invalid \\x escape")?;
            push_byte(out, value);
            return Ok(i + 3);
        }
        b'c' => {
            let letter = ecma
                .get(i + 1)
                .filter(|letter| letter.is_ascii_alphabetic())
                .ok_or("invalid \\c escape")?;
            push_byte(out, letter % 32);
            return Ok(i + 2);
        }
        b'u' => return Err("\\u escapes are not supported over bytes"),
        _ => push_byte(out, c),
    }
    Ok(i + 1)
}

fn hex_value(digits: Option<&[u8]>) -> Option<u8> {
    let digits = std::str::from_utf8(digits?).ok()?;
    if digits.len() != 2 {
        return None;
    }
    u8::from_str_radix(digits, 16).ok()
}

/// Translate an ECMAScript class whose `[` is just before `ecma[i]`;
/// returns the index after its `]`.
fn translate_class(ecma: &[u8], mut i: usize, out: &mut String) -> Result<usize, &'static str> {
    let negated = ecma.get(i) == Some(&b'^');
    if negated {
        i += 1;
    }
    // ECMAScript `[]` matches nothing and `[^]` matches anything.
    if ecma.get(i) == Some(&b']') {
        out.push_str(if negated {
            r"[\x00-\xFF]"
        } else {
            r"[^\x00-\xFF]"
        });
        return Ok(i + 1);
    }
    out.push_str(if negated { "[^" } else { "[" });
    loop {
        let Some(&c) = ecma.get(i) else {
            return Err("unterminated bracket expression");
        };
        i += 1;
        match c {
            b']' => {
                out.push(']');
                return Ok(i);
            }
            b'\\' => i = translate_escape(ecma, i, true, out)?,
            b'-' => out.push('-'),
            b'[' if ecma.get(i) == Some(&b':') => {
                let start = i + 1;
                let length = ecma[start..]
                    .windows(2)
                    .position(|pair| pair == b":]")
                    .ok_or("unterminated character class name")?;
                let name = std::str::from_utf8(&ecma[start..start + length])
                    .ok()
                    .filter(|name| POSIX_CLASSES.contains(name))
                    .ok_or("unknown character class name")?;
                out.push_str("[:");
                out.push_str(name);
                out.push_str(":]");
                i = start + length + 2;
            }
            _ => push_byte(out, c),
        }
    }
}

/// The POSIX class names both `std::regex` and the `regex` crate know.
const POSIX_CLASSES: &[&str] = &[
    "alnum", "alpha", "blank", "cntrl", "digit", "graph", "lower", "print", "punct", "space",
    "upper", "xdigit",
];

fn push_byte(out: &mut String, byte: u8) {
    use std::fmt::Write as _;
    let _ = write!(out, "\\x{byte:02X}");
}
