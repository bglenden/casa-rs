// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `spw` expression grammar, a port of `ms/MSSel/MSSpwGram.yy`.
//!
//! bison builds the grammar without conflicts (its `%nonassoc` and `%right`
//! declarations resolve none), so the language is exactly the context-free
//! grammar, which this recursive-descent parser accepts:
//!
//! ```text
//! SpwStatement  : FullExpr | "(" FullExpr ")"
//! FullExpr      : FullSpec ("," FullSpec)*
//! FullSpec      : Spw (":" FreqList)?
//! Spw           : IDENTIFIER | QSTRING | REGEX | ">" OneFreq | "<" OneFreq
//!               | "~" OneFreq | OneFreq "<>" OneFreq | FreqList
//! FreqList      : FListElements (";" FListElements)*
//! FListElements : IndexRange | PhyRange | OneFreq
//! OneFreq       : PhyVal | PhyVal UNIT
//! IndexRange    : PhyVal "~" PhyVal ("^" PhyVal)* | "^" PhyVal ("^" PhyVal)*
//! PhyRange      : PhyVal UNIT "~" PhyVal UNIT ("^" PhyVal UNIT)*
//!               | PhyVal "~" PhyVal UNIT ("^" PhyVal UNIT)*
//! ```
//!
//! The actions' values are C `Float`s and `Int`s, kept here as `f32` with
//! the same single-precision arithmetic. The checks the actions make on
//! their own operands (a reversed range, a velocity unit, a lone `^` with a
//! unit) are made while parsing; the actions that read the
//! SPECTRAL_WINDOW table run afterwards ([`super::actions`]). Every
//! refusal refuses the whole expression, so only the message can depend on
//! that order.

use super::lexer::{Kind, Lexer, Token};

/// What an element's numbers mean: the codes of `MSSpwIndex::MSSpwTypes`.
/// `MSSPW_INDEX` and `MSSPW_INDEXRANGE` are treated alike everywhere.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Unit {
    /// Channel or window indices.
    Index,
    /// Frequencies in hertz (`MSSPW_UNITHZ`).
    Hertz,
}

/// One `FListElements`: the four floats the grammar stores per element
/// (`MSSpwGram.yy:232-265`). A single index or frequency has `start ==
/// stop` and step 0; `^n` alone has start and stop -1.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct Element {
    pub(super) start: f32,
    pub(super) stop: f32,
    pub(super) step: f32,
    pub(super) unit: Unit,
}

/// A `OneFreq`: an index, truncated to an integer, or a frequency in hertz.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct OneFreq {
    pub(super) value: f32,
    pub(super) unit: Unit,
}

/// The `Spw` forms (`MSSpwGram.yy:267-409`).
#[derive(Clone, Debug, PartialEq)]
pub(super) enum Spw {
    /// `IDENTIFIER`: the windows whose NAME equals it.
    Name(String),
    /// `QSTRING` (quoted, or bare with `*` or `?`) and `REGEX`: a pattern.
    Pattern(String),
    /// `>x`.
    Greater(OneFreq),
    /// `<x`.
    Less(OneFreq),
    /// `x<>y`.
    Between(OneFreq, OneFreq),
    /// `~x`.
    Near(OneFreq),
    /// A list of window indices or frequencies.
    List(Vec<Element>),
}

/// One `FullSpec`: the windows and, after `:`, their channels.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct Spec {
    pub(super) spw: Spw,
    pub(super) channels: Option<Vec<Element>>,
}

/// A parsed expression.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct Expression {
    pub(super) specs: Vec<Spec>,
    /// `( ... )`: the rule that skips `endOfCeremony`, so the expression
    /// puts no condition on MAIN rows (`MSSpwGram.yy:104-105`).
    pub(super) parenthesized: bool,
}

/// Parse `text`, or return CASA's message for why it does not parse.
pub(super) fn parse(text: &str) -> Result<Expression, String> {
    let mut parser = Parser {
        lexer: Lexer::new(text),
        token: Token {
            kind: Kind::End,
            lexeme: "",
            value: "".into(),
        },
    };
    parser.advance();
    let parenthesized = parser.token.kind == Kind::LParen;
    if parenthesized {
        parser.advance();
    }
    let mut specs = vec![parser.spec()?];
    while parser.token.kind == Kind::Comma {
        parser.advance();
        specs.push(parser.spec()?);
    }
    if parenthesized {
        parser.expect(Kind::RParen)?;
    }
    parser.expect(Kind::End)?;
    Ok(Expression {
        specs,
        parenthesized,
    })
}

struct Parser<'s> {
    lexer: Lexer<'s>,
    token: Token<'s>,
}

/// An element as parsed, before it is known whether a lone `OneFreq` is
/// the start of `OneFreq "<>" OneFreq`.
enum Parsed {
    One(OneFreq),
    Range(Element),
}

impl Parsed {
    fn element(self) -> Element {
        match self {
            // `FListElements: OneFreq` (`.yy:239-245`).
            Self::One(one) => Element {
                start: one.value,
                stop: one.value,
                step: 0.0,
                unit: one.unit,
            },
            Self::Range(element) => element,
        }
    }
}

impl<'s> Parser<'s> {
    fn advance(&mut self) -> Token<'s> {
        let next = self.lexer.next_token();
        std::mem::replace(&mut self.token, next)
    }

    /// `MSSpwGramerror` (`MSSpwGram.cc:208-212`).
    fn syntax_error(&self) -> String {
        format!(
            "Spw Expression: Parse error at or near '{}'",
            self.token.lexeme
        )
    }

    fn expect(&mut self, kind: Kind) -> Result<Token<'s>, String> {
        if self.token.kind == kind {
            Ok(self.advance())
        } else {
            Err(self.syntax_error())
        }
    }

    fn spec(&mut self) -> Result<Spec, String> {
        let spw = self.spw()?;
        let channels = if self.token.kind == Kind::Colon {
            self.advance();
            Some(self.list(None)?)
        } else {
            None
        };
        Ok(Spec { spw, channels })
    }

    fn spw(&mut self) -> Result<Spw, String> {
        match self.token.kind {
            Kind::Identifier => Ok(Spw::Name(self.advance().value.into_owned())),
            Kind::QString | Kind::Regex => Ok(Spw::Pattern(self.advance().value.into_owned())),
            Kind::Gt => {
                self.advance();
                Ok(Spw::Greater(self.one_freq()?))
            }
            Kind::Lt => {
                self.advance();
                Ok(Spw::Less(self.one_freq()?))
            }
            Kind::Dash => {
                self.advance();
                Ok(Spw::Near(self.one_freq()?))
            }
            Kind::Number | Kind::Caret => {
                let first = self.element()?;
                match first {
                    Parsed::One(low) if self.token.kind == Kind::GtNLt => {
                        self.advance();
                        Ok(Spw::Between(low, self.one_freq()?))
                    }
                    first => Ok(Spw::List(self.list(Some(first))?)),
                }
            }
            _ => Err(self.syntax_error()),
        }
    }

    /// `FreqList`, continuing from an element already parsed.
    fn list(&mut self, first: Option<Parsed>) -> Result<Vec<Element>, String> {
        let first = match first {
            Some(first) => first,
            None => self.element()?,
        };
        let mut elements = vec![first.element()];
        while self.token.kind == Kind::Semicolon {
            self.advance();
            elements.push(self.element()?.element());
        }
        Ok(elements)
    }

    /// `OneFreq` (`.yy:219-231`).
    fn one_freq(&mut self) -> Result<OneFreq, String> {
        let value = self.number()?;
        if self.token.kind == Kind::Unit {
            let factor = self.unit()?;
            Ok(OneFreq {
                value: value * factor,
                unit: Unit::Hertz,
            })
        } else {
            Ok(index(value)?)
        }
    }

    /// `FListElements` (`.yy:141-246`).
    fn element(&mut self) -> Result<Parsed, String> {
        if self.token.kind == Kind::Caret {
            self.advance();
            let step = self.number()?;
            if self.token.kind == Kind::Unit {
                self.unit()?;
                return Err("Spw expression: A lone \"^PhyUnits\" not yet supported.".to_string());
            }
            // `IndexRange: CARET PhyVal` keeps the step as written.
            let range = Element {
                start: -1.0,
                stop: -1.0,
                step,
                unit: Unit::Index,
            };
            return self.index_steps(range).map(Parsed::Range);
        }
        let low = self.number()?;
        match self.token.kind {
            Kind::Unit => {
                let factor = self.unit()?;
                let low = low * factor;
                if self.token.kind != Kind::Dash {
                    return Ok(Parsed::One(OneFreq {
                        value: low,
                        unit: Unit::Hertz,
                    }));
                }
                // `PhyRange: Physical DASH Physical`.
                self.advance();
                let high = self.number()?;
                if self.token.kind != Kind::Unit {
                    return Err(self.syntax_error());
                }
                let high = high * self.unit()?;
                if low > high {
                    return Err(reversed());
                }
                self.physical_steps(low, high).map(Parsed::Range)
            }
            Kind::Dash => {
                self.advance();
                let high = self.number()?;
                if self.token.kind == Kind::Unit {
                    // `PhyRange: PhyVal DASH PhyVal UnitCode`.
                    let factor = self.unit()?;
                    if low > high {
                        return Err(reversed());
                    }
                    return self
                        .physical_steps(low * factor, high * factor)
                        .map(Parsed::Range);
                }
                // `IndexRange: PhyVal DASH PhyVal`.
                if low > high {
                    return Err(reversed());
                }
                let range = Element {
                    start: integer(low)?,
                    stop: integer(high)?,
                    step: 0.0,
                    unit: Unit::Index,
                };
                self.index_steps(range).map(Parsed::Range)
            }
            _ => Ok(Parsed::One(index(low)?)),
        }
    }

    /// `IndexRange: IndexRange CARET PhyVal`, each step truncated.
    fn index_steps(&mut self, mut range: Element) -> Result<Element, String> {
        while self.token.kind == Kind::Caret {
            self.advance();
            range.step = integer(self.number()?)?;
            if self.token.kind == Kind::Unit {
                return Err(self.syntax_error());
            }
        }
        Ok(range)
    }

    /// `PhyRange` and `PhyRange CARET Physical`.
    fn physical_steps(&mut self, start: f32, stop: f32) -> Result<Element, String> {
        let mut range = Element {
            start,
            stop,
            step: 0.0,
            unit: Unit::Hertz,
        };
        while self.token.kind == Kind::Caret {
            self.advance();
            let step = self.number()?;
            if self.token.kind != Kind::Unit {
                return Err(self.syntax_error());
            }
            range.step = step * self.unit()?;
        }
        Ok(range)
    }

    /// `PhyVal: FNUMBER`, read by `sscanf("%f")` (`.yy:108-115`).
    fn number(&mut self) -> Result<f32, String> {
        if self.token.kind != Kind::Number {
            return Err(self.syntax_error());
        }
        let token = self.advance();
        scan_float(token.lexeme).ok_or_else(|| {
            format!(
                "Spw expression: sscanf cannot read a number from '{}', so CASA would use an \
                 uninitialised value",
                token.lexeme
            )
        })
    }

    /// `UnitCode: UNIT` (`.yy:117-134`): the factor to hertz of a frequency
    /// unit; a velocity unit is refused.
    fn unit(&mut self) -> Result<f32, String> {
        let token = self.expect(Kind::Unit)?;
        unit_factor(token.lexeme)
    }
}

/// `MSSpwIndex::convertToMKS(1.0, 1.0, unit)(0)` (`MSSpwIndex.cc:375-390`)
/// for a unit holding `hz`: a `Float` factor chosen by the unit's first
/// letter, so `mHz` is a megahertz and `THz` is the single-precision
/// `1e12`.
fn unit_factor(lexeme: &str) -> Result<f32, String> {
    let lower = lexeme.to_ascii_lowercase();
    if !lower.contains("hz") {
        return Err("Spw expression: Velocity units support temporarily disabled.".to_string());
    }
    Ok(match lower.as_bytes()[0] {
        b'k' => 1000.0,
        b'm' => 1.0e6,
        b'g' => 1.0e9,
        b't' => 1.0e12,
        _ => 1.0,
    })
}

fn reversed() -> String {
    "Spw expression: Start of range greater than end of range".to_string()
}

/// `OneFreq: PhyVal` (`.yy:219-224`): the index `(Int)$1`.
fn index(value: f32) -> Result<OneFreq, String> {
    Ok(OneFreq {
        value: integer(value)?,
        unit: Unit::Index,
    })
}

/// A C `(Int)` cast of a `Float`, back in a `Float`. Outside `Int` the cast
/// is undefined (x86 and ARM disagree), so it is refused.
pub(super) fn integer(value: f32) -> Result<f32, String> {
    let wide = f64::from(value);
    if wide > -2_147_483_649.0 && wide < 2_147_483_648.0 {
        Ok(value.trunc())
    } else {
        Err(format!(
            "Spw expression: {value} is outside a C Int, where CASA's (Int) cast is undefined"
        ))
    }
}

/// `sscanf(text, "%f", &f)`: the longest prefix `strtof` reads, rounded to
/// the nearest `float`. `D` and `d` are no exponent there, so `1d5` reads
/// 1. `None` where no digits precede the exponent, so nothing is read.
pub(super) fn scan_float(text: &str) -> Option<f32> {
    let bytes = text.as_bytes();
    let mut end = usize::from(matches!(bytes.first(), Some(b'+' | b'-')));
    let integer_digits = bytes[end..]
        .iter()
        .take_while(|b| b.is_ascii_digit())
        .count();
    end += integer_digits;
    let mut fraction_digits = 0;
    if bytes.get(end) == Some(&b'.') {
        fraction_digits = bytes[end + 1..]
            .iter()
            .take_while(|b| b.is_ascii_digit())
            .count();
        if integer_digits + fraction_digits > 0 {
            end += 1 + fraction_digits;
        }
    }
    if integer_digits + fraction_digits == 0 {
        return None;
    }
    if matches!(bytes.get(end), Some(b'e' | b'E')) {
        let sign = usize::from(matches!(bytes.get(end + 1), Some(b'+' | b'-')));
        let digits = bytes.get(end + 1 + sign..).map_or(0, |rest| {
            rest.iter().take_while(|b| b.is_ascii_digit()).count()
        });
        if digits > 0 {
            end += 1 + sign + digits;
        }
    }
    text[..end].parse::<f32>().ok()
}
