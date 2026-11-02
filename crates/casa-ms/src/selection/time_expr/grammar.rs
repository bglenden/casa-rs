// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `timerange` lexer and parser.
//!
//! This is casacore's `MSTimeGram.ll` (flex) and `MSTimeGram.yy` (bison) as a
//! recursive-descent parser. Bison builds that grammar's LALR(1) table with no
//! conflicts and no precedence-resolved choices, so the parser below, which
//! decides on at most two tokens of lookahead where the table decides on one
//! token after a reduction, accepts exactly the same strings and produces the
//! same semantic values. It stops at the parse: defaults, validation and time
//! arithmetic are the grammar actions' calls into `MSTimeParse`, which
//! [`super`] performs.

use crate::{MsError, MsResult};

/// The grammar's `TimeFields` (`MSTimeDefinitions.h`): calendar fields held as
/// C `Int`, where [`UNSET`] asks `MSTimeParse::setDefaults` for a default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct TimeFields {
    pub(super) year: i32,
    pub(super) month: i32,
    pub(super) day: i32,
    pub(super) hour: i32,
    pub(super) minute: i32,
    pub(super) sec: i32,
    /// Milliseconds: the grammar's `fsec`.
    pub(super) millisec: i32,
}

/// The `-1` that `*` and an omitted field leave (`wildNumber: STAR {$$ = -1;}`).
pub(super) const UNSET: i32 = -1;

/// How the two times of a range are joined (`rangetimeexpr`,
/// MSTimeGram.yy:153-186).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RangeJoin {
    /// `T0~T1`: the second time takes its missing fields from the first.
    Span,
    /// `T0+dT`: the second time is a duration added to the first.
    Duration,
}

/// The edge treatment of a range: the grammar's `MSTimeEdgeInclusiveRange`
/// and `MSTimeEdgeBuffer` (MSTimeGram.yy:93-94, 148-152).
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum Edge {
    /// No brackets: a closed range.
    Closed,
    /// `[...]`: edges widened by half the default exposure.
    Exposure,
    /// `N[...]`: edges widened by `N` seconds, held as the C `Float`
    /// `MSTimeEdgeBuffer`.
    Width(f32),
}

/// One comma-separated `timeexpr` (MSTimeGram.yy:125-129), unresolved.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum TermSyntax {
    /// `singletimeexpr`: one time.
    Single(TimeFields),
    /// `lowboundtimeexpr`: `>T`.
    AtOrAfter(TimeFields),
    /// `upboundtimeexpr`: `<T`.
    AtOrBefore(TimeFields),
    /// `brangetimeexpr`: a range, bracketed or not.
    Range {
        lower: TimeFields,
        join: RangeJoin,
        upper: TimeFields,
        edge: Edge,
    },
}

/// Parse a non-empty `timerange` expression into its terms.
pub(super) fn parse(text: &str) -> MsResult<Vec<TermSyntax>> {
    let mut parser = Parser {
        text,
        lexemes: lex(text),
        cursor: 0,
    };
    parser.statement()
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Token {
    /// `{NUMBER}`: a run of digits, valued as C `atoi` values it.
    Number(i32),
    /// `{FNUMBER}` with a point (`12.`, `.5`, `12.5`), valued by `atof`. A run
    /// of digits alone is a `NUMBER`: flex gives a tie to the earlier rule.
    Float(f64),
    Colon,
    Greater,
    Less,
    Slash,
    Plus,
    /// `~`, which the grammar calls `DASH`.
    Tilde,
    Comma,
    Star,
    Open,
    Close,
    /// A token the lexer knows but no grammar rule accepts (`<=`, `&&`, `||`,
    /// `!`, `.`, `%`, `'`), or a character it does not know (`UNKNOWN`).
    Rejected,
    End,
}

#[derive(Clone, Copy, Debug)]
struct Lexeme {
    token: Token,
    start: usize,
    end: usize,
    /// `msTimeGramPosition()` once the lexer has read this token: the bytes
    /// of every earlier rule except `UNKNOWN`, which does not count itself.
    position: usize,
}

/// casacore `MSTimeGram.ll`, longest match first and the earlier rule on a tie.
fn lex(text: &str) -> Vec<Lexeme> {
    let bytes = text.as_bytes();
    let mut lexemes = Vec::new();
    let mut cursor = 0;
    let mut position = 0;
    while cursor < bytes.len() {
        let start = cursor;
        let next = bytes.get(cursor + 1).copied();
        let (token, end) = match bytes[cursor] {
            b' ' | b'\t' | b'\n' => {
                cursor += 1;
                position += 1;
                continue;
            }
            b':' => (Token::Colon, start + 1),
            b'>' => (Token::Greater, start + 1),
            b'<' if next == Some(b'=') => (Token::Rejected, start + 2),
            b'<' => (Token::Less, start + 1),
            b'&' if next == Some(b'&') => (Token::Rejected, start + 2),
            b'|' if next == Some(b'|') => (Token::Rejected, start + 2),
            b'.' if next.is_some_and(|byte| byte.is_ascii_digit()) => lex_number(text, start),
            b'!' | b'.' | b'%' | b'\'' => (Token::Rejected, start + 1),
            b'/' => (Token::Slash, start + 1),
            b'+' => (Token::Plus, start + 1),
            b'~' => (Token::Tilde, start + 1),
            b',' => (Token::Comma, start + 1),
            b'*' => (Token::Star, start + 1),
            b'[' => (Token::Open, start + 1),
            b']' => (Token::Close, start + 1),
            b'0'..=b'9' => lex_number(text, start),
            _ => {
                // flex's catch-all `.` returns UNKNOWN without counting it.
                let width = text[start..].chars().next().map_or(1, char::len_utf8);
                cursor = start + width;
                lexemes.push(Lexeme {
                    token: Token::Rejected,
                    start,
                    end: cursor,
                    position,
                });
                continue;
            }
        };
        position += end - start;
        cursor = end;
        lexemes.push(Lexeme {
            token,
            start,
            end,
            position,
        });
    }
    lexemes.push(Lexeme {
        token: Token::End,
        start: bytes.len(),
        end: bytes.len(),
        position,
    });
    lexemes
}

/// `{NUMBER}` or `{FNUMBER}` starting at `start`, a digit or a point before one.
fn lex_number(text: &str, start: usize) -> (Token, usize) {
    let bytes = text.as_bytes();
    let digits_end = skip_digits(bytes, start);
    if bytes.get(digits_end) != Some(&b'.') {
        return (Token::Number(atoi(&text[start..digits_end])), digits_end);
    }
    let end = skip_digits(bytes, digits_end + 1);
    // `atof` of `INT.`, `.INT` or `INT.INT`; Rust and glibc both round correctly.
    let value = text[start..end]
        .parse::<f64>()
        .expect("a decimal literal with digits around a point parses");
    (Token::Float(value), end)
}

fn skip_digits(bytes: &[u8], mut cursor: usize) -> usize {
    while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
        cursor += 1;
    }
    cursor
}

/// C `atoi` as glibc and macOS's libc compute it, `(int) strtol(s, 0, 10)`:
/// `strtol` saturates at the 64-bit `LONG_MAX` and the conversion to `int`
/// keeps the low 32 bits, so `99999999999999999999` becomes `-1`.
fn atoi(digits: &str) -> i32 {
    let value = digits.parse::<i64>().unwrap_or(i64::MAX);
    #[expect(
        clippy::cast_possible_truncation,
        reason = "C's long-to-int conversion keeps the low 32 bits"
    )]
    let low = value as i32;
    low
}

/// The time of day a `timeObj` gives (MSTimeGram.yy:215-220). The grammar
/// holds it as three `Double`s, but the hour and minute are always the `Int`
/// values of `tFields` or `-1`, so they keep that type here.
#[derive(Clone, Copy, Debug)]
struct Clock {
    hour: i32,
    minute: i32,
    seconds: f64,
}

struct Parser<'a> {
    text: &'a str,
    lexemes: Vec<Lexeme>,
    cursor: usize,
}

impl Parser<'_> {
    fn peek(&self) -> Token {
        self.lexemes[self.cursor].token
    }

    fn peek_second(&self) -> Token {
        self.lexemes
            .get(self.cursor + 1)
            .map_or(Token::End, |lexeme| lexeme.token)
    }

    fn advance(&mut self) {
        if self.peek() != Token::End {
            self.cursor += 1;
        }
    }

    /// `MSTimeGramerror` at the current token, with the position
    /// `baseMSTimeGramParseCommand` adds (MSTimeGram.cc:86-109, 230-235).
    fn error(&self) -> MsError {
        let lexeme = self.lexemes[self.cursor];
        MsError::InvalidInput(format!(
            "MSSelection time error: Parse error at or near token '{}' \
             (near char. {} in string \"{}\")",
            &self.text[lexeme.start..lexeme.end],
            lexeme.position + 1,
            self.text
        ))
    }

    fn expect(&mut self, token: Token) -> MsResult<()> {
        if self.peek() == token {
            self.advance();
            Ok(())
        } else {
            Err(self.error())
        }
    }

    /// `timestatement` (MSTimeGram.yy:108-123): terms joined by commas.
    fn statement(&mut self) -> MsResult<Vec<TermSyntax>> {
        let mut terms = vec![self.term()?];
        while self.peek() == Token::Comma {
            self.advance();
            terms.push(self.term()?);
        }
        self.expect(Token::End)?;
        Ok(terms)
    }

    /// `timeexpr` (MSTimeGram.yy:125-206).
    fn term(&mut self) -> MsResult<TermSyntax> {
        match (self.peek(), self.peek_second()) {
            (Token::Greater, _) => {
                self.advance();
                Ok(TermSyntax::AtOrAfter(self.year_time()?))
            }
            (Token::Less, _) => {
                self.advance();
                Ok(TermSyntax::AtOrBefore(self.year_time()?))
            }
            (Token::Open, _) => {
                self.advance();
                self.bracketed(Edge::Exposure)
            }
            // `LBRACKET: FNUMBER LSQBRACKET`: the width must be an FNUMBER,
            // so `3[...]` is refused and `3.[...]` is not.
            (Token::Float(width), Token::Open) => {
                self.advance();
                self.advance();
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "MSTimeEdgeBuffer is a C Float"
                )]
                let buffer = width as f32;
                self.bracketed(Edge::Width(buffer))
            }
            // Bison shifts a leading FNUMBER, which only `[` may follow, and
            // fails at the token after it (`57813.375` fails at its end).
            (Token::Float(_), _) => {
                self.advance();
                Err(self.error())
            }
            _ => {
                let lower = self.year_time()?;
                let Some(join) = self.range_join() else {
                    return Ok(TermSyntax::Single(lower));
                };
                let upper = self.year_time()?;
                Ok(TermSyntax::Range {
                    lower,
                    join,
                    upper,
                    edge: Edge::Closed,
                })
            }
        }
    }

    /// `LBRACKET rangetimeexpr RSQBRACKET`, after the bracket.
    fn bracketed(&mut self, edge: Edge) -> MsResult<TermSyntax> {
        let lower = self.year_time()?;
        let Some(join) = self.range_join() else {
            return Err(self.error());
        };
        let upper = self.year_time()?;
        self.expect(Token::Close)?;
        Ok(TermSyntax::Range {
            lower,
            join,
            upper,
            edge,
        })
    }

    fn range_join(&mut self) -> Option<RangeJoin> {
        let join = match self.peek() {
            Token::Tilde => RangeJoin::Span,
            Token::Plus => RangeJoin::Duration,
            _ => return None,
        };
        self.advance();
        Some(join)
    }

    /// `yeartimeexpr` (MSTimeGram.yy:228-262).
    fn year_time(&mut self) -> MsResult<TimeFields> {
        let starts_date = match self.peek() {
            Token::Slash => true,
            token => is_wild(token) && self.peek_second() == Token::Slash,
        };
        if !starts_date {
            let clock = self.clock()?;
            return clock_fields(UNSET, UNSET, UNSET, clock);
        }
        // `yearObj` (MSTimeGram.yy:221-227): `Y/M/D`, or `M/D/` with year 0.
        let first = self.date_field()?;
        let second = self.date_field()?;
        let (year, month, day) = match self.wild_number() {
            Some(day) => (first, second, day),
            None => (0, first, second),
        };
        if self.peek() != Token::Slash {
            return Ok(TimeFields {
                year,
                month,
                day,
                hour: UNSET,
                minute: UNSET,
                sec: UNSET,
                millisec: UNSET,
            });
        }
        self.advance();
        let clock = self.clock()?;
        clock_fields(year, month, day, clock)
    }

    /// `yFields` (MSTimeGram.yy:211-213): `N/`, `*/` or a bare `/` (0).
    fn date_field(&mut self) -> MsResult<i32> {
        let value = self.wild_number().unwrap_or(0);
        self.expect(Token::Slash)?;
        Ok(value)
    }

    /// `tFields` (MSTimeGram.yy:207-209): `N:`, `*:` or a bare `:` (0).
    fn clock_field(&mut self) -> MsResult<i32> {
        let value = self.wild_number().unwrap_or(0);
        self.expect(Token::Colon)?;
        Ok(value)
    }

    fn clock_field_ahead(&self) -> bool {
        match self.peek() {
            Token::Colon => true,
            token => is_wild(token) && self.peek_second() == Token::Colon,
        }
    }

    /// `timeObj` (MSTimeGram.yy:215-220). One colon reads as minutes and
    /// seconds, a trailing colon alone as seconds, and a bare number as
    /// seconds with the hour and minute defaulted.
    fn clock(&mut self) -> MsResult<Clock> {
        if !self.clock_field_ahead() {
            let Some(seconds) = self.wild_number() else {
                return Err(self.error());
            };
            return Ok(Clock {
                hour: UNSET,
                minute: UNSET,
                seconds: f64::from(seconds),
            });
        }
        let first = self.clock_field()?;
        if self.clock_field_ahead() {
            let second = self.clock_field()?;
            return Ok(Clock {
                hour: first,
                minute: second,
                seconds: self.wild_float().unwrap_or(0.0),
            });
        }
        Ok(match self.wild_float() {
            Some(seconds) => Clock {
                hour: 0,
                minute: first,
                seconds,
            },
            None => Clock {
                hour: 0,
                minute: 0,
                seconds: f64::from(first),
            },
        })
    }

    /// `wildNumber` (MSTimeGram.yy:131-133), if one is next.
    fn wild_number(&mut self) -> Option<i32> {
        let value = match self.peek() {
            Token::Star => UNSET,
            Token::Number(value) => value,
            _ => return None,
        };
        self.advance();
        Some(value)
    }

    /// `wildFloat` (MSTimeGram.yy:134-136), if one is next.
    fn wild_float(&mut self) -> Option<f64> {
        if let Token::Float(value) = self.peek() {
            self.advance();
            return Some(value);
        }
        self.wild_number().map(f64::from)
    }
}

fn is_wild(token: Token) -> bool {
    matches!(token, Token::Star | Token::Number(_))
}

/// `msTimeGramSetTimeFields` with the seconds split by `splitSec`
/// (MSTimeGram.yy:97-104, 228-261).
fn clock_fields(year: i32, month: i32, day: i32, clock: Clock) -> MsResult<TimeFields> {
    let (sec, millisec) = split_seconds(clock.seconds)?;
    Ok(TimeFields {
        year,
        month,
        day,
        hour: clock.hour,
        minute: clock.minute,
        sec,
        millisec,
    })
}

/// `splitSec` (MSTimeGram.yy:97-104): whole seconds and milliseconds, both
/// truncated, so `1.001` (`1.000999...` as a double) keeps 0 ms. A
/// negative second (`*` gives `-1`) leaves the milliseconds unset too.
///
/// A seconds value beyond C `Int` is undefined behaviour in the C++ cast and
/// differs between CASA's platforms, so it is refused here.
fn split_seconds(seconds: f64) -> MsResult<(i32, i32)> {
    if !(seconds > -2_147_483_649.0 && seconds < 2_147_483_648.0) {
        return Err(MsError::InvalidInput(format!(
            "MSSelection time error: seconds value {seconds} is beyond the C Int \
             range CASA converts it to"
        )));
    }
    #[expect(
        clippy::cast_possible_truncation,
        reason = "C's (Int) cast truncates; the range is checked above"
    )]
    let sec = seconds as i32;
    if sec < 0 {
        return Ok((sec, UNSET));
    }
    #[expect(
        clippy::cast_possible_truncation,
        reason = "C's (Int) cast truncates a value in [0, 1000)"
    )]
    let millisec = ((seconds - f64::from(sec)) * 1000.0) as i32;
    Ok((sec, millisec))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields(text: &str) -> TimeFields {
        match parse(text).unwrap()[..] {
            [TermSyntax::Single(fields)] => fields,
            ref other => panic!("{text:?} parsed as {other:?}"),
        }
    }

    fn tf(
        year: i32,
        month: i32,
        day: i32,
        hour: i32,
        minute: i32,
        sec: i32,
        ms: i32,
    ) -> TimeFields {
        TimeFields {
            year,
            month,
            day,
            hour,
            minute,
            sec,
            millisec: ms,
        }
    }

    #[test]
    fn time_objects_take_casas_field_positions() {
        // MSTimeGram.yy:215-219 and 228-261.
        assert_eq!(
            fields("2017/03/01/09:30:15.5"),
            tf(2017, 3, 1, 9, 30, 15, 500)
        );
        assert_eq!(fields("09:30:"), tf(-1, -1, -1, 9, 30, 0, 0));
        // One colon is minutes and seconds, not hours and minutes.
        assert_eq!(fields("09:30"), tf(-1, -1, -1, 0, 9, 30, 0));
        // A trailing colon alone is seconds.
        assert_eq!(fields("09:"), tf(-1, -1, -1, 0, 0, 9, 0));
        // A bare number is seconds with the hour and minute defaulted.
        assert_eq!(fields("9"), tf(-1, -1, -1, -1, -1, 9, 0));
        assert_eq!(fields("2017/03/01/9"), tf(2017, 3, 1, -1, -1, 9, 0));
        assert_eq!(fields("2017/03/01"), tf(2017, 3, 1, -1, -1, -1, -1));
        // Empty fields are 0; `*` is -1 and a starred second unsets the ms.
        assert_eq!(fields("::"), tf(-1, -1, -1, 0, 0, 0, 0));
        assert_eq!(fields("*:*:*"), tf(-1, -1, -1, -1, -1, -1, -1));
        assert_eq!(fields("*/*/*"), tf(-1, -1, -1, -1, -1, -1, -1));
        assert_eq!(fields("/03/01"), tf(0, 3, 1, -1, -1, -1, -1));
        // `M/D/` puts the two numbers in the month and day, with year 0.
        assert_eq!(fields("03/01//09:00:00"), tf(0, 3, 1, 9, 0, 0, 0));
    }

    #[test]
    fn seconds_truncate_to_whole_milliseconds() {
        // splitSec (MSTimeGram.yy:97-104): (Int)((fsec - sec) * 1000).
        // 1.001 is the double 1.000999999999999889... and keeps 0 ms.
        assert_eq!(fields("09:00:01.001").millisec, 0);
        assert_eq!(fields("09:00:02.501").millisec, 500);
        assert_eq!(fields("09:00:12.345").millisec, 345);
        assert_eq!(fields("09:00:00.5009").millisec, 500);
        assert_eq!(fields("09:00:00.").millisec, 0);
        assert_eq!(fields("09:00:.25"), tf(-1, -1, -1, 9, 0, 0, 250));
    }

    #[test]
    fn numbers_follow_atoi() {
        assert_eq!(fields("007"), tf(-1, -1, -1, -1, -1, 7, 0));
        // strtol saturates to LONG_MAX, whose low 32 bits are -1 (a default).
        assert_eq!(
            fields("99999999999999999999"),
            tf(-1, -1, -1, -1, -1, -1, -1)
        );
        assert_eq!(atoi("4294967297"), 1);
        assert_eq!(atoi("2147483648"), i32::MIN);
    }

    #[test]
    fn ranges_brackets_and_lists() {
        let terms = parse("09:00:00~10:00:00, [1:0:0+5] ,2.5[1~2],>3,<4").unwrap();
        assert_eq!(terms.len(), 5);
        assert!(matches!(
            terms[0],
            TermSyntax::Range {
                join: RangeJoin::Span,
                edge: Edge::Closed,
                ..
            }
        ));
        assert!(matches!(
            terms[1],
            TermSyntax::Range {
                join: RangeJoin::Duration,
                edge: Edge::Exposure,
                ..
            }
        ));
        assert!(matches!(
            terms[2],
            TermSyntax::Range {
                edge: Edge::Width(2.5),
                ..
            }
        ));
        assert!(matches!(terms[3], TermSyntax::AtOrAfter(_)));
        assert!(matches!(terms[4], TermSyntax::AtOrBefore(_)));
    }

    #[test]
    fn edge_widths_are_single_precision() {
        // MSTimeEdgeBuffer is a C Float (MSTimeGram.yy:94).
        let [TermSyntax::Range { edge, .. }] = parse("0.1[1~2]").unwrap()[..] else {
            panic!("expected one range");
        };
        assert_eq!(edge, Edge::Width(0.1_f32));
    }

    #[test]
    fn refusals_report_casas_token_and_position() {
        for (text, token, position) in [
            ("3[09:00:00~09:01:00]", "[", 3),
            ("[09:00:00]", "]", 11),
            ("2017-03-01", "-", 5),
            ("abc", "a", 1),
            (" ", "", 2),
            ("09:00:00~", "", 10),
            ("09:00:00.5+2.5", "2.5", 15),
            ("09:00:00+10s", "s", 12),
            ("57813.375", "", 10),
            ("09:00:00.5&&09:00:04.5", "&&", 13),
            ("2017/03/01 09:00:00", "09", 14),
            ("<=09:00:00", "<=", 3),
            ("'09:00:00'", "'", 2),
        ] {
            let message = parse(text).unwrap_err().to_string();
            let expected = format!(
                "Parse error at or near token '{token}' (near char. {position} in string \"{text}\")"
            );
            assert!(message.contains(&expected), "{text:?}: {message}");
        }
    }

    #[test]
    fn seconds_beyond_c_int_are_refused() {
        let message = parse("09:00:3000000000.5").unwrap_err().to_string();
        assert!(message.contains("beyond the C Int range"), "{message}");
    }
}
