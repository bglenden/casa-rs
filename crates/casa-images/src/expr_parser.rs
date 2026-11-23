// SPDX-License-Identifier: LGPL-3.0-or-later
//! Parser for casacore-compatible Lattice Expression Language (LEL) strings.
//!
//! Converts a LEL expression string into the lazy [`ImageExprBuilder`] / [`MaskExprBuilder`]
//! DAG.  The supported grammar covers the full C++ LEL surface (Waves 11-14):
//!
//! - **Arithmetic**: `+`, `-`, `*`, `/`, `^` (power, right-associative)
//! - **Unary**: `-x`, `+x`
//! - **Comparison**: `>`, `<`, `>=`, `<=`, `==`, `!=`
//! - **Logical**: `&&`, `||`, `!`
//! - **1-arg math**: `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `sinh`,
//!   `cosh`, `tanh`, `exp`, `log`, `log10`, `sqrt`, `abs`, `ceil`, `floor`,
//!   `round`, `sign`, `conj`
//! - **1-arg mask**: `isnan` (→ mask), `all`, `any` (mask → mask)
//! - **1-arg numeric**: `ntrue`, `nfalse` (mask → scalar), `sum`, `min1d`,
//!   `max1d`, `mean1d`/`mean`, `median1d`/`median` (reductions)
//! - **1-arg metadata**: `ndim`, `nelem`/`nelements` (→ scalar), `mask`, `value`
//! - **2-arg functions**: `pow`, `fmod`, `atan2`, `min`, `max`, `length`,
//!   `fractile1d`/`fractile`, `fractilerange1d`/`fractilerange`, `replace`
//! - **3-arg functions**: `iif` (conditional), `fractilerange1d` (3-arg variant)
//! - **0-arg constants**: `pi()`, `e()`
//!
//! Type-changing functions (`real`, `imag`, `arg`, `complex`) are available
//! only via the typed API on [`ImageExprBuilder`], not through the parser (the parser
//! is monomorphic in T). `indexin` is deferred (requires array literal syntax).
//! - **Numeric literals**: integers, floats (e.g. `1.5e-3`)
//! - **Image references**: quoted paths (`'path'`, `"path"`) or unquoted
//!   identifiers resolved via the [`ImageResolver`] trait.
//!
//! # Grammar (EBNF)
//!
//! ```text
//! expression  → or_expr
//! or_expr     → and_expr ('||' and_expr)*
//! and_expr    → not_expr ('&&' not_expr)*
//! not_expr    → '!' not_expr | rel_expr
//! rel_expr    → add_expr (('==' | '!=' | '>' | '>=' | '<' | '<=') add_expr)?
//! add_expr    → mul_expr (('+' | '-') mul_expr)*
//! mul_expr    → power_expr (('*' | '/') power_expr)*
//! power_expr  → unary_expr ('^' power_expr)?        // right-associative
//! unary_expr  → ('-' | '+') unary_expr | call_expr
//! call_expr   → IDENT '(' arglist? ')' | primary
//! primary     → '(' expression ')' | NUMBER | IMAGE_REF
//! ```
//!
//! # Operator precedence (lowest → highest)
//!
//! | Level | Operators          | Associativity |
//! |-------|--------------------|---------------|
//! | 1     | `\|\|`             | left          |
//! | 2     | `&&`               | left          |
//! | 3     | `!`                | prefix        |
//! | 4     | `== != > >= < <=`  | non-assoc     |
//! | 5     | `+ -`              | left          |
//! | 6     | `* /`              | left          |
//! | 7     | `^`                | right         |
//! | 8     | unary `- +`        | prefix        |
//!
//! # Path quoting rules
//!
//! Image paths may be specified using single quotes, double quotes, or as bare
//! identifiers (alphanumeric plus `.`, `_`, `/`, `~`).  Inside quoted strings
//! the opposite quote character is literal; there is no backslash-escape
//! mechanism inside quotes in the supported subset.  Adjacent quoted strings
//! are **not** concatenated (unlike the full C++ lexer); each quoted token is
//! a single image reference.
//!
//! # Deferred features (Wave 14 / beyond)
//!
//! - `$n` temporary lattice references
//! - Region references (`::region`)
//! - Array literals `[...]`
//! - `indexin` / `indexnotin`
//! - Reduction functions (single-arg `min`, `max`, `mean`, `median`, etc.)
//! - Type conversion functions (`float`, `double`, `complex`, etc.)
//! - `iif(cond, true, false)` conditional
//! - Complex literal suffix `i`
//! - `%` modulo infix operator (use `fmod()` instead)
//!
//! # Example
//!
//! ```rust
//! use std::collections::HashMap;
//! use casa_coordinates::CoordinateSystem;
//! use casa_images::{ImageExprBuilder, TempImage};
//! use casa_images::expr_parser::{parse_image_expr, HashMapResolver};
//! use casa_lattices::LatticeMut;
//!
//! let mut a = TempImage::<f32>::new(vec![4, 4], CoordinateSystem::new(), casa_lattices::TempStoragePolicy::Memory).unwrap();
//! a.set(2.0).unwrap();
//!
//! let mut images = HashMap::new();
//! images.insert("a".to_string(), &a as &dyn casa_images::image::ImageInterface<f32>);
//! let resolver = HashMapResolver(images);
//!
//! let expr = parse_image_expr("sin('a') + 1.0", &resolver).unwrap();
//! let val = expr.get_at(&[0, 0]).unwrap();
//! assert!((val - (2.0_f32.sin() + 1.0)).abs() < 1e-6);
//! ```

use std::collections::HashMap;
use std::fmt;

use crate::error::ImageError;
use crate::image::ImageInterface;
use crate::image_expr::{
    ImageExprBinaryOp, ImageExprBuilder, ImageExprCompareOp, ImageExprUnaryOp, ImageExprValue,
    ImageExpression, MaskExprBuilder, MaskExpression, ReductionOp,
};
use crate::image_expr_ops::{apply_binary, apply_unary};

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors that can occur while parsing a LEL expression string.
///
/// Corresponds conceptually to `casacore::AipsError` thrown by
/// `ImageExprParse::command` in C++.
#[derive(Debug, Clone, PartialEq)]
pub struct ExprParseError {
    /// Human-readable description.
    pub message: String,
    /// Byte offset into the expression string where the error was detected.
    pub position: usize,
}

impl fmt::Display for ExprParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "expression parse error at position {}: {}",
            self.position, self.message
        )
    }
}

impl std::error::Error for ExprParseError {}

impl From<ExprParseError> for ImageError {
    fn from(e: ExprParseError) -> Self {
        ImageError::InvalidMetadata(e.to_string())
    }
}

// ---------------------------------------------------------------------------
// Image resolver trait
// ---------------------------------------------------------------------------

/// Resolves image name tokens (quoted paths or bare identifiers) to image
/// references usable by the expression DAG.
///
/// Implement this trait to plug in your own image lookup strategy.  A blanket
/// implementation is provided for closures with the matching signature, and
/// [`HashMapResolver`] provides a simple dictionary-based resolver.
pub trait ImageResolver<'a, T: ImageExprValue> {
    /// Look up an image by its name/path as it appeared in the expression
    /// string (with quotes stripped).
    fn resolve(&self, name: &str) -> Result<&'a dyn ImageInterface<T>, ImageError>;
}

impl<'a, T, F> ImageResolver<'a, T> for F
where
    T: ImageExprValue,
    F: Fn(&str) -> Result<&'a dyn ImageInterface<T>, ImageError>,
{
    fn resolve(&self, name: &str) -> Result<&'a dyn ImageInterface<T>, ImageError> {
        (self)(name)
    }
}

/// Simple [`HashMap`]-backed image resolver.
///
/// Maps image name strings to trait-object references.  Useful for tests and
/// interactive exploration where images are pre-opened.
pub struct HashMapResolver<'a, T: ImageExprValue>(pub HashMap<String, &'a dyn ImageInterface<T>>);

impl<'a, T: ImageExprValue> ImageResolver<'a, T> for HashMapResolver<'a, T> {
    fn resolve(&self, name: &str) -> Result<&'a dyn ImageInterface<T>, ImageError> {
        self.0
            .get(name)
            .copied()
            .ok_or_else(|| ImageError::InvalidMetadata(format!("unknown image: {name}")))
    }
}

// ---------------------------------------------------------------------------
// Token types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Token {
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

struct Lexer<'s> {
    src: &'s str,
    pos: usize,
}

impl<'s> Lexer<'s> {
    fn new(src: &'s str) -> Self {
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

    fn next_token(&mut self) -> Result<(Token, usize), ExprParseError> {
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

// ---------------------------------------------------------------------------
// Parser intermediate representation
// ---------------------------------------------------------------------------

mod complex;
mod read_geometry;

use casa_types::Complex64;
use complex::{arithmetic_complex, compare_complex, complex_constructor, complex_function};
use read_geometry::Operator;

#[derive(Clone, Copy)]
struct ScalarLiteral {
    value: f64,
    single_precision: bool,
}

impl ScalarLiteral {
    fn double(value: f64) -> Self {
        Self {
            value,
            single_precision: false,
        }
    }

    fn single(value: f32) -> Self {
        Self {
            value: f64::from(value),
            single_precision: true,
        }
    }
}

/// Intermediate parse result: either a numeric expression or a boolean mask.
enum ExprNode<'a, T: ImageExprValue + PartialOrd + ExprValueConvert> {
    Numeric(Box<ImageExprBuilder<'a, T>>, bool),
    Complex(Box<ImageExprBuilder<'a, Complex64>>, bool),
    Scalar(ScalarLiteral),
    Mask(MaskExprBuilder<'a, T>),
    IntArray(Vec<i64>),
}

impl<'a, T: ImageExprValue + PartialOrd + ExprValueConvert> ExprNode<'a, T> {
    fn numeric(builder: ImageExprBuilder<'a, T>, single: bool) -> Self {
        Self::Numeric(Box::new(builder), single)
    }

    fn single_precision(&self) -> bool {
        match self {
            Self::Scalar(value) => value.single_precision,
            Self::Numeric(_, single) | Self::Complex(_, single) => *single,
            _ => false,
        }
    }

    fn into_numeric(self, pos: usize) -> Result<ImageExprBuilder<'a, T>, ExprParseError> {
        match self {
            Self::Numeric(e, _) => Ok(*e),
            Self::Scalar(_) => Err(ExprParseError {
                message: "bare scalar cannot be used as an image expression (no shape context)"
                    .into(),
                position: pos,
            }),
            Self::Mask(_) => Err(ExprParseError {
                message: "expected numeric expression but found boolean mask".into(),
                position: pos,
            }),
            Self::Complex(..) => Err(ExprParseError {
                message: "expected real expression but found a complex image".into(),
                position: pos,
            }),
            Self::IntArray(_) => Err(ExprParseError {
                message: "expected numeric expression but found integer array literal".into(),
                position: pos,
            }),
        }
    }

    fn into_mask(self, pos: usize) -> Result<MaskExprBuilder<'a, T>, ExprParseError> {
        match self {
            Self::Mask(m) => Ok(m),
            _ => Err(ExprParseError {
                message: "expected boolean mask expression but found numeric".into(),
                position: pos,
            }),
        }
    }

    /// A numeric expression, with a bare scalar accepted as a constant
    /// one: the shape context comes from the other operands, as LEL
    /// allows.
    fn into_numeric_or_constant(
        self,
        pos: usize,
    ) -> Result<ImageExprBuilder<'a, T>, ExprParseError> {
        match self {
            Self::Numeric(expression, _) => Ok(*expression),
            Self::Scalar(value) => Ok(ImageExprBuilder::scalar(T::from_f64(value.value))),
            Self::Mask(_) | Self::IntArray(_) | Self::Complex(..) => Err(ExprParseError {
                message: "expected numeric expression".into(),
                position: pos,
            }),
        }
    }
}

fn round_real_reduction<'a, T: ImageExprValue + ExprValueConvert>(
    expression: ImageExprBuilder<'a, T>,
    single: bool,
) -> ImageExprBuilder<'a, T> {
    if single {
        expression.custom_unary("float reduction", |value: T| {
            T::from_f64(f64::from(value.to_f64() as f32))
        })
    } else {
        expression
    }
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

struct Parser<'a, 's, T: ImageExprValue + PartialOrd + ExprValueConvert, R: ImageResolver<'a, T>> {
    lexer: Lexer<'s>,
    current: Token,
    current_pos: usize,
    resolver: &'s R,
    complex_resolver: Option<&'s dyn ImageResolver<'a, Complex64>>,
    casa_scalar_literals: bool,
    fill_empty_reductions: bool,
    check_lattice_geometry: bool,
    integer_array_argument: bool,
    single_precision_sources: Option<&'s dyn Fn(&str) -> bool>,
    _marker: std::marker::PhantomData<&'a T>,
}

impl<'a, 's, T, R> Parser<'a, 's, T, R>
where
    T: ImageExprValue + PartialOrd + ExprValueConvert,
    R: ImageResolver<'a, T>,
{
    fn new(src: &'s str, resolver: &'s R) -> Result<Self, ExprParseError> {
        let mut lexer = Lexer::new(src);
        let (tok, pos) = lexer.next_token()?;
        Ok(Self {
            lexer,
            current: tok,
            current_pos: pos,
            resolver,
            complex_resolver: None,
            casa_scalar_literals: false,
            fill_empty_reductions: false,
            check_lattice_geometry: false,
            integer_array_argument: false,
            single_precision_sources: None,
            _marker: std::marker::PhantomData,
        })
    }

    fn advance(&mut self) -> Result<(), ExprParseError> {
        let (tok, pos) = self.lexer.next_token()?;
        self.current = tok;
        self.current_pos = pos;
        Ok(())
    }

    fn expect(&mut self, expected: &Token) -> Result<(), ExprParseError> {
        if &self.current == expected {
            self.advance()
        } else {
            Err(ExprParseError {
                message: format!("expected {expected} but found {}", self.current),
                position: self.current_pos,
            })
        }
    }

    fn convert_f64_to_t(value: f64) -> T {
        T::from_f64(value)
    }

    // Combine two nodes with a binary arithmetic op, handling scalar promotion.
    fn combine_binary(
        &self,
        lhs: ExprNode<'a, T>,
        rhs: ExprNode<'a, T>,
        op: ImageExprBinaryOp,
        pos: usize,
    ) -> Result<ExprNode<'a, T>, ExprParseError> {
        self.validate_lattice_operands(&lhs, &rhs, pos, Operator::Infix)?;
        if matches!(&lhs, ExprNode::Complex(..)) || matches!(&rhs, ExprNode::Complex(..)) {
            return arithmetic_complex(lhs, rhs, op, pos);
        }
        if lhs.single_precision()
            && rhs.single_precision()
            && (!matches!(&lhs, ExprNode::Scalar(_)) || !matches!(&rhs, ExprNode::Scalar(_)))
        {
            return lhs
                .into_numeric_or_constant(pos)?
                .binary_expr_with_name(
                    rhs.into_numeric_or_constant(pos)?,
                    "float arithmetic",
                    std::sync::Arc::new(move |a: T, b: T| {
                        let output = apply_binary(op, a.to_f64() as f32, b.to_f64() as f32);
                        T::from_f64(f64::from(output))
                    }),
                )
                .map(|expression| ExprNode::numeric(expression, true))
                .map_err(|error| ExprParseError {
                    message: error.to_string(),
                    position: pos,
                });
        }
        match (&lhs, &rhs) {
            (ExprNode::Scalar(a), ExprNode::Scalar(b)) => {
                // Both scalars: compute at parse time
                let result = if a.single_precision && b.single_precision {
                    ScalarLiteral::single(apply_binary(op, a.value as f32, b.value as f32))
                } else {
                    let a = Self::convert_f64_to_t(a.value);
                    let b = Self::convert_f64_to_t(b.value);
                    ScalarLiteral::double(apply_binary(op, a, b).to_f64())
                };
                Ok(ExprNode::Scalar(result))
            }
            (ExprNode::Scalar(s), ExprNode::Numeric(img, _)) => {
                // scalar op image
                match op {
                    // Commutative ops: rewrite as image op scalar
                    ImageExprBinaryOp::Add
                    | ImageExprBinaryOp::Multiply
                    | ImageExprBinaryOp::Min
                    | ImageExprBinaryOp::Max => Ok(ExprNode::numeric(
                        (**img)
                            .clone()
                            .binary_scalar(Self::convert_f64_to_t(s.value), op),
                        false,
                    )),
                    // Non-commutative: use scalar_left_binary for correct Inf/NaN handling
                    _ => Ok(ExprNode::numeric(
                        ImageExprBuilder::scalar_left_binary(
                            Self::convert_f64_to_t(s.value),
                            (**img).clone(),
                            op,
                        ),
                        false,
                    )),
                }
            }
            (ExprNode::Numeric(img, _), ExprNode::Scalar(s)) => Ok(ExprNode::numeric(
                (**img)
                    .clone()
                    .binary_scalar(Self::convert_f64_to_t(s.value), op),
                false,
            )),
            (ExprNode::Numeric(_, _), ExprNode::Numeric(_, _)) => {
                let lhs_expr = lhs.into_numeric(pos)?;
                let rhs_expr = rhs.into_numeric(pos)?;
                Ok(ExprNode::numeric(
                    lhs_expr
                        .binary_expr(rhs_expr, op)
                        .map_err(|e| ExprParseError {
                            message: e.to_string(),
                            position: pos,
                        })?,
                    false,
                ))
            }
            _ => Err(ExprParseError {
                message: "cannot apply arithmetic operator to boolean mask expression".into(),
                position: pos,
            }),
        }
    }

    // -----------------------------------------------------------------------
    // Recursive descent
    // -----------------------------------------------------------------------

    fn parse_expression(&mut self) -> Result<ExprNode<'a, T>, ExprParseError> {
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> Result<ExprNode<'a, T>, ExprParseError> {
        let mut left = self.parse_and_expr()?;
        while self.current == Token::Or {
            let pos = self.current_pos;
            self.advance()?;
            let right = self.parse_and_expr()?;
            self.validate_lattice_operands(&left, &right, pos, Operator::Logical)?;
            let lhs_mask = left.into_mask(pos)?;
            let rhs_mask = right.into_mask(pos)?;
            left = ExprNode::Mask(lhs_mask.or(rhs_mask).map_err(|e| ExprParseError {
                message: e.to_string(),
                position: pos,
            })?);
        }
        Ok(left)
    }

    fn parse_and_expr(&mut self) -> Result<ExprNode<'a, T>, ExprParseError> {
        let mut left = self.parse_not_expr()?;
        while self.current == Token::And {
            let pos = self.current_pos;
            self.advance()?;
            let right = self.parse_not_expr()?;
            self.validate_lattice_operands(&left, &right, pos, Operator::Logical)?;
            let lhs_mask = left.into_mask(pos)?;
            let rhs_mask = right.into_mask(pos)?;
            left = ExprNode::Mask(lhs_mask.and(rhs_mask).map_err(|e| ExprParseError {
                message: e.to_string(),
                position: pos,
            })?);
        }
        Ok(left)
    }

    fn parse_not_expr(&mut self) -> Result<ExprNode<'a, T>, ExprParseError> {
        if self.current == Token::Bang {
            let pos = self.current_pos;
            self.advance()?;
            let inner = self.parse_not_expr()?;
            let mask = inner.into_mask(pos)?;
            return Ok(ExprNode::Mask(mask.logical_not()));
        }
        self.parse_rel_expr()
    }

    fn parse_rel_expr(&mut self) -> Result<ExprNode<'a, T>, ExprParseError> {
        let left = self.parse_add_expr()?;
        let cmp_op = match &self.current {
            Token::Eq => Some(ImageExprCompareOp::Equal),
            Token::Ne => Some(ImageExprCompareOp::NotEqual),
            Token::Gt => Some(ImageExprCompareOp::GreaterThan),
            Token::Ge => Some(ImageExprCompareOp::GreaterEqual),
            Token::Lt => Some(ImageExprCompareOp::LessThan),
            Token::Le => Some(ImageExprCompareOp::LessEqual),
            _ => None,
        };
        if let Some(op) = cmp_op {
            let pos = self.current_pos;
            self.advance()?;
            let right = self.parse_add_expr()?;
            self.validate_lattice_operands(&left, &right, pos, Operator::Infix)?;

            if matches!(&left, ExprNode::Complex(..)) || matches!(&right, ExprNode::Complex(..)) {
                return compare_complex(left, right, op, pos);
            }
            match (&left, &right) {
                (ExprNode::Scalar(lhs), ExprNode::Scalar(rhs)) => {
                    let (lhs, rhs) = (lhs.value, rhs.value);
                    let value = match op {
                        ImageExprCompareOp::GreaterThan => lhs > rhs,
                        ImageExprCompareOp::LessThan => lhs < rhs,
                        ImageExprCompareOp::GreaterEqual => lhs >= rhs,
                        ImageExprCompareOp::LessEqual => lhs <= rhs,
                        ImageExprCompareOp::Equal => lhs == rhs,
                        ImageExprCompareOp::NotEqual => lhs != rhs,
                    };
                    return Ok(ExprNode::Mask(MaskExprBuilder::from_constant(
                        ndarray::arr0(value).into_dyn(),
                    )));
                }
                (ExprNode::Numeric(img, _), ExprNode::Scalar(s)) => {
                    let mask = (**img)
                        .clone()
                        .compare_scalar(Self::convert_f64_to_t(s.value), op);
                    return Ok(ExprNode::Mask(mask));
                }
                (ExprNode::Scalar(s), ExprNode::Numeric(img, _)) => {
                    // Flip the comparison: scalar op image => image flipped_op scalar
                    let flipped = flip_compare_op(op);
                    let mask = (**img)
                        .clone()
                        .compare_scalar(Self::convert_f64_to_t(s.value), flipped);
                    return Ok(ExprNode::Mask(mask));
                }
                (ExprNode::Numeric(lhs, _), ExprNode::Numeric(rhs, _)) => {
                    let mask = (**lhs)
                        .clone()
                        .compare_expr((**rhs).clone(), op)
                        .map_err(|e| ExprParseError {
                            message: e.to_string(),
                            position: pos,
                        })?;
                    return Ok(ExprNode::Mask(mask));
                }
                _ => {
                    return Err(ExprParseError {
                        message: "cannot compare boolean mask expressions".into(),
                        position: pos,
                    });
                }
            }
        }
        Ok(left)
    }

    fn parse_add_expr(&mut self) -> Result<ExprNode<'a, T>, ExprParseError> {
        let mut left = self.parse_mul_expr()?;
        loop {
            let op = match &self.current {
                Token::Plus => ImageExprBinaryOp::Add,
                Token::Minus => ImageExprBinaryOp::Subtract,
                _ => break,
            };
            let pos = self.current_pos;
            self.advance()?;
            let right = self.parse_mul_expr()?;
            left = self.combine_binary(left, right, op, pos)?;
        }
        Ok(left)
    }

    fn parse_mul_expr(&mut self) -> Result<ExprNode<'a, T>, ExprParseError> {
        let mut left = self.parse_power_expr()?;
        loop {
            let op = match &self.current {
                Token::Star => ImageExprBinaryOp::Multiply,
                Token::Slash => ImageExprBinaryOp::Divide,
                Token::Percent => ImageExprBinaryOp::Fmod,
                _ => break,
            };
            let pos = self.current_pos;
            self.advance()?;
            let right = self.parse_power_expr()?;
            left = self.combine_binary(left, right, op, pos)?;
        }
        Ok(left)
    }

    fn parse_power_expr(&mut self) -> Result<ExprNode<'a, T>, ExprParseError> {
        let base = self.parse_unary_expr()?;
        if self.current == Token::Caret {
            let pos = self.current_pos;
            self.advance()?;
            // Right-associative: recurse into power_expr
            let exponent = self.parse_power_expr()?;
            return self.combine_binary(base, exponent, ImageExprBinaryOp::Pow, pos);
        }
        Ok(base)
    }

    fn parse_unary_expr(&mut self) -> Result<ExprNode<'a, T>, ExprParseError> {
        match &self.current {
            Token::Minus => {
                let pos = self.current_pos;
                self.advance()?;
                let inner = self.parse_unary_expr()?;
                match inner {
                    ExprNode::Scalar(mut literal) => {
                        literal.value = -literal.value;
                        Ok(ExprNode::Scalar(literal))
                    }
                    ExprNode::Numeric(expr, single) => {
                        Ok(ExprNode::numeric((*expr).negate(), single))
                    }
                    ExprNode::Complex(expr, single) => {
                        Ok(ExprNode::Complex(Box::new((*expr).negate()), single))
                    }
                    ExprNode::Mask(_) | ExprNode::IntArray(_) => Err(ExprParseError {
                        message: "cannot negate a non-numeric expression".into(),
                        position: pos,
                    }),
                }
            }
            Token::Plus => {
                self.advance()?;
                self.parse_unary_expr()
            }
            _ => self.parse_call_expr(),
        }
    }

    fn parse_call_expr(&mut self) -> Result<ExprNode<'a, T>, ExprParseError> {
        if let Token::Ident(name) = self.current.clone() {
            let pos = self.current_pos;
            // Check if this is a function call (ident followed by '(')
            self.advance()?;
            if self.current == Token::LParen {
                return self.parse_function_call(&name, pos);
            }
            if matches!(name.as_str(), "T" | "F") {
                let value = name == "T";
                return Ok(ExprNode::Mask(MaskExprBuilder::from_constant(
                    ndarray::arr0(value).into_dyn(),
                )));
            }
            return self.resolve_image(&name, pos);
        }
        self.parse_primary()
    }

    fn parse_function_call(
        &mut self,
        name: &str,
        pos: usize,
    ) -> Result<ExprNode<'a, T>, ExprParseError> {
        // We are positioned at '('
        self.expect(&Token::LParen)?;

        // Check for 0-arg functions
        if self.current == Token::RParen {
            self.advance()?;
            return self.eval_function_0(name, pos);
        }

        // Parse first argument
        let arg1 = self.parse_expression()?;

        // Check for 1-arg
        if self.current == Token::RParen {
            self.advance()?;
            return self.eval_function_1(name, arg1, pos);
        }

        // Expect comma and second argument
        self.expect(&Token::Comma)?;
        let previous_indices = self.integer_array_argument;
        self.integer_array_argument =
            name.eq_ignore_ascii_case("indexin") || name.eq_ignore_ascii_case("rebin");
        let arg2 = self.parse_expression();
        self.integer_array_argument = previous_indices;
        let arg2 = arg2?;

        if self.current == Token::RParen {
            self.advance()?;
            return self.eval_function_2(name, arg1, arg2, pos);
        }

        // Check for 3-arg function
        self.expect(&Token::Comma)?;
        let arg3 = self.parse_expression()?;

        if self.current == Token::RParen {
            self.advance()?;
            return self.eval_function_3(name, arg1, arg2, arg3, pos);
        }

        Err(ExprParseError {
            message: format!("function '{name}' takes at most 3 arguments"),
            position: pos,
        })
    }

    fn eval_function_0(&self, name: &str, pos: usize) -> Result<ExprNode<'a, T>, ExprParseError> {
        let lower = name.to_ascii_lowercase();
        match lower.as_str() {
            "pi" => Ok(ExprNode::Scalar(ScalarLiteral::double(
                std::f64::consts::PI,
            ))),
            "e" => Ok(ExprNode::Scalar(ScalarLiteral::double(std::f64::consts::E))),
            _ => Err(ExprParseError {
                message: format!("unknown 0-argument function: {name}"),
                position: pos,
            }),
        }
    }

    fn eval_function_1(
        &self,
        name: &str,
        arg: ExprNode<'a, T>,
        pos: usize,
    ) -> Result<ExprNode<'a, T>, ExprParseError> {
        let lower = name.to_ascii_lowercase();
        let unary_op = match lower.as_str() {
            "sin" => Some(ImageExprUnaryOp::Sin),
            "cos" => Some(ImageExprUnaryOp::Cos),
            "tan" => Some(ImageExprUnaryOp::Tan),
            "asin" => Some(ImageExprUnaryOp::Asin),
            "acos" => Some(ImageExprUnaryOp::Acos),
            "atan" => Some(ImageExprUnaryOp::Atan),
            "sinh" => Some(ImageExprUnaryOp::Sinh),
            "cosh" => Some(ImageExprUnaryOp::Cosh),
            "tanh" => Some(ImageExprUnaryOp::Tanh),
            "exp" => Some(ImageExprUnaryOp::Exp),
            "log" => Some(ImageExprUnaryOp::Log),
            "log10" => Some(ImageExprUnaryOp::Log10),
            "sqrt" => Some(ImageExprUnaryOp::Sqrt),
            "abs" => Some(ImageExprUnaryOp::Abs),
            "amplitude" if !T::is_complex() => Some(ImageExprUnaryOp::Abs),
            "ceil" => Some(ImageExprUnaryOp::Ceil),
            "floor" => Some(ImageExprUnaryOp::Floor),
            "round" => Some(ImageExprUnaryOp::Round),
            "sign" => Some(ImageExprUnaryOp::Sign),
            "conj" => Some(ImageExprUnaryOp::Conj),
            _ => None,
        };

        if let ExprNode::Complex(expr, single) = arg {
            return complex_function(&lower, *expr, single, unary_op, pos);
        }
        let reduction = match lower.as_str() {
            "sum" => Some(ReductionOp::Sum),
            "min1d" | "min" => Some(ReductionOp::Min),
            "max1d" | "max" => Some(ReductionOp::Max),
            "mean1d" | "mean" => Some(ReductionOp::Mean),
            "median1d" | "median" => Some(ReductionOp::Median),
            "variance" | "variance1d" => Some(ReductionOp::Variance),
            "stddev" | "stddev1d" => Some(ReductionOp::Stddev),
            "avdev" | "avdev1d" => Some(ReductionOp::Avdev),
            _ => None,
        };
        if let Some(op) = reduction {
            return self.eval_reduction(arg, op, pos);
        }
        if let Some(op) = unary_op {
            match arg {
                ExprNode::Scalar(literal) => {
                    let result = if literal.single_precision {
                        ScalarLiteral::single(apply_unary(op, literal.value as f32))
                    } else {
                        let value = Self::convert_f64_to_t(literal.value);
                        ScalarLiteral::double(apply_unary(op, value).to_f64())
                    };
                    Ok(ExprNode::Scalar(result))
                }
                ExprNode::Numeric(expr, true) => Ok(ExprNode::numeric(
                    expr.custom_unary("float arithmetic", move |value: T| {
                        T::from_f64(f64::from(apply_unary(op, value.to_f64() as f32)))
                    }),
                    true,
                )),
                ExprNode::Numeric(expr, false) => Ok(ExprNode::numeric((*expr).unary(op), false)),
                ExprNode::Mask(_) | ExprNode::IntArray(_) | ExprNode::Complex(..) => {
                    Err(ExprParseError {
                        message: format!("function '{name}' requires a numeric argument"),
                        position: pos,
                    })
                }
            }
        } else {
            // Wave 14 functions
            match lower.as_str() {
                "isnan" => match arg {
                    ExprNode::Scalar(v) => {
                        // Constant-fold: isnan of a literal
                        let is_nan = v.value.is_nan();
                        // We need a shape context to produce a MaskExprBuilder; without an image
                        // we cannot create one. Return error like other scalar-only cases.
                        Err(ExprParseError {
                            message: if is_nan {
                                "isnan() of a NaN scalar has no image shape context".into()
                            } else {
                                "isnan() of a scalar has no image shape context".into()
                            },
                            position: pos,
                        })
                    }
                    ExprNode::Numeric(expr, _) => Ok(ExprNode::Mask((*expr).isnan())),
                    ExprNode::Mask(_) | ExprNode::IntArray(_) | ExprNode::Complex(..) => {
                        Err(ExprParseError {
                            message: "isnan() requires a numeric argument".into(),
                            position: pos,
                        })
                    }
                },
                "ndim" => {
                    let expr = arg.into_numeric(pos)?;
                    Ok(ExprNode::Scalar(ScalarLiteral::double(
                        expr.ndim_value() as f64
                    )))
                }
                "nelem" | "nelements" => {
                    let expr = arg.into_numeric(pos)?;
                    Ok(ExprNode::Scalar(ScalarLiteral::double(
                        expr.nelem_value() as f64
                    )))
                }
                // Wave 14c: mask-aware 1-arg functions
                "all" => {
                    let mask = arg.into_mask(pos)?;
                    Ok(ExprNode::Mask(mask.all_reduce()))
                }
                "any" => {
                    let mask = arg.into_mask(pos)?;
                    Ok(ExprNode::Mask(mask.any_reduce()))
                }
                "ntrue" => {
                    let mask = arg.into_mask(pos)?;
                    Ok(ExprNode::numeric(ImageExprBuilder::ntrue(mask), false))
                }
                "nfalse" => {
                    let mask = arg.into_mask(pos)?;
                    Ok(ExprNode::numeric(ImageExprBuilder::nfalse(mask), false))
                }
                "mask" => match arg {
                    ExprNode::Mask(mask) => Ok(ExprNode::Mask(mask.validity())),
                    other => Ok(ExprNode::Mask(
                        other.into_numeric_or_constant(pos)?.validity(),
                    )),
                },
                "value" => match arg {
                    ExprNode::Numeric(expr, single) => {
                        Ok(ExprNode::numeric((*expr).strip_mask(), single))
                    }
                    ExprNode::Scalar(_) => Ok(arg),
                    ExprNode::Mask(_) | ExprNode::IntArray(_) | ExprNode::Complex(..) => {
                        Err(ExprParseError {
                            message: "value() requires a numeric argument".into(),
                            position: pos,
                        })
                    }
                },
                "float" if !T::is_complex() => match arg {
                    ExprNode::Scalar(value) => {
                        Ok(ExprNode::Scalar(ScalarLiteral::single(value.value as f32)))
                    }
                    ExprNode::Numeric(expr, _) => Ok(ExprNode::numeric(
                        expr.custom_unary("float", |value| {
                            T::from_f64(f64::from(value.to_f64() as f32))
                        }),
                        true,
                    )),
                    _ => Err(ExprParseError {
                        message: "'float()' requires a numeric argument".into(),
                        position: pos,
                    }),
                },
                "real" | "double" if !T::is_complex() => match arg {
                    ExprNode::Scalar(literal) if lower == "double" => {
                        Ok(ExprNode::Scalar(ScalarLiteral::double(literal.value)))
                    }
                    ExprNode::Numeric(expr, _) if lower == "double" => {
                        Ok(ExprNode::numeric(*expr, false))
                    }
                    ExprNode::Numeric(_, _) | ExprNode::Scalar(_) => Ok(arg),
                    ExprNode::Mask(_) | ExprNode::IntArray(_) | ExprNode::Complex(..) => {
                        Err(ExprParseError {
                            message: format!("'{lower}()' requires a numeric argument"),
                            position: pos,
                        })
                    }
                },
                // Wave 14d: type-changing functions (typed API only for complex types)
                "amplitude" | "real" | "imag" | "arg" | "complex" => Err(ExprParseError {
                    message: format!(
                        "'{lower}()' changes pixel type and is available as a typed API method only \
                         (e.g. expr.real_part()), not through the parser"
                    ),
                    position: pos,
                }),
                _ => Err(ExprParseError {
                    message: format!("unknown 1-argument function: {name}"),
                    position: pos,
                }),
            }
        }
    }

    fn eval_reduction(
        &self,
        arg: ExprNode<'a, T>,
        op: ReductionOp,
        pos: usize,
    ) -> Result<ExprNode<'a, T>, ExprParseError> {
        let single = arg.single_precision();
        let empty_value = self.fill_empty_reductions.then(|| {
            let value = if matches!(
                op,
                ReductionOp::Min | ReductionOp::Max | ReductionOp::Mean | ReductionOp::Median
            ) {
                if single {
                    -f64::from(f32::MIN_POSITIVE)
                } else {
                    -f64::MIN_POSITIVE
                }
            } else {
                0.0
            };
            T::from_f64(value)
        });
        let input = if self.casa_scalar_literals {
            arg.into_numeric_or_constant(pos)?
        } else {
            arg.into_numeric(pos)?
        };
        let expression = input.make_reduction_with_empty_value(op, empty_value);
        Ok(ExprNode::numeric(
            round_real_reduction(expression, single),
            single,
        ))
    }

    fn eval_function_2(
        &self,
        name: &str,
        arg1: ExprNode<'a, T>,
        arg2: ExprNode<'a, T>,
        pos: usize,
    ) -> Result<ExprNode<'a, T>, ExprParseError> {
        let lower = name.to_ascii_lowercase();
        let binary_op = match lower.as_str() {
            "pow" => Some(ImageExprBinaryOp::Pow),
            "fmod" => Some(ImageExprBinaryOp::Fmod),
            "atan2" => Some(ImageExprBinaryOp::Atan2),
            "min" => Some(ImageExprBinaryOp::Min),
            "max" => Some(ImageExprBinaryOp::Max),
            _ => None,
        };

        if let Some(op) = binary_op {
            self.validate_lattice_operands(&arg1, &arg2, pos, Operator::Function)?;
            return self.combine_binary(arg1, arg2, op, pos);
        }

        // Wave 14 2-arg functions
        match lower.as_str() {
            "complex" => complex_constructor(arg1, arg2, pos),
            "length" => {
                let expr = arg1.into_numeric(pos)?;
                let axis = match arg2 {
                    ExprNode::Scalar(v) => v.value as usize,
                    _ => {
                        return Err(ExprParseError {
                            message: "length() second argument must be a scalar axis index".into(),
                            position: pos,
                        });
                    }
                };
                match expr.length_value(axis) {
                    Some(len) => Ok(ExprNode::Scalar(ScalarLiteral::double(len as f64))),
                    None => Err(ExprParseError {
                        message: format!(
                            "axis {axis} out of range for {}-d image",
                            expr.ndim_value()
                        ),
                        position: pos,
                    }),
                }
            }
            "fractile1d" | "fractile" => {
                let expr = arg1.into_numeric(pos)?;
                let frac = match arg2 {
                    ExprNode::Scalar(v) => v.value,
                    _ => {
                        return Err(ExprParseError {
                            message: "fractile1d() second argument must be a scalar fraction"
                                .into(),
                            position: pos,
                        });
                    }
                };
                Ok(ExprNode::numeric(expr.fractile(frac), false))
            }
            "fractilerange1d" | "fractilerange" => {
                // 2-arg variant: fractilerange1d(expr, frac) = range from frac to 1-frac
                let expr = arg1.into_numeric(pos)?;
                let frac = match arg2 {
                    ExprNode::Scalar(v) => v.value,
                    _ => {
                        return Err(ExprParseError {
                            message: "fractilerange1d() second argument must be a scalar fraction"
                                .into(),
                            position: pos,
                        });
                    }
                };
                Ok(ExprNode::numeric(
                    expr.fractile_range(frac, 1.0 - frac),
                    false,
                ))
            }
            "replace" => {
                // replace(image, replacement) — replaces masked-out pixels while keeping mask.
                let single = arg1.single_precision() && arg2.single_precision();
                let primary = arg1.into_numeric(pos)?;
                let replacement = arg2.into_numeric_or_constant(pos)?;
                primary
                    .replace_invalid(replacement)
                    .map_err(|e| ExprParseError {
                        message: e.to_string(),
                        position: pos,
                    })
                    .map(|r| ExprNode::numeric(r, single))
            }
            "spectralindex" => {
                if T::is_complex() {
                    return Err(ExprParseError {
                        message: "spectralindex() requires real pixel types".into(),
                        position: pos,
                    });
                }
                let single = arg1.single_precision() && arg2.single_precision();
                let lhs = arg1.into_numeric(pos)?;
                let rhs = arg2.into_numeric(pos)?;
                lhs.spectral_index_with_metadata(rhs, !self.casa_scalar_literals)
                    .map_err(|e| ExprParseError {
                        message: e.to_string(),
                        position: pos,
                    })
                    .map(|r| ExprNode::numeric(r, single))
            }
            "pa" => {
                if T::is_complex() {
                    return Err(ExprParseError {
                        message: "pa() requires real pixel types".into(),
                        position: pos,
                    });
                }
                let u_is_num = matches!(arg1, ExprNode::Numeric(_, _));
                let q_is_num = matches!(arg2, ExprNode::Numeric(_, _));
                if !u_is_num && !q_is_num {
                    return Err(ExprParseError {
                        message: "pa() requires at least one image expression".into(),
                        position: pos,
                    });
                }
                let u = arg1.into_numeric_or_constant(pos)?;
                let q = arg2.into_numeric_or_constant(pos)?;
                ImageExprBuilder::pa_with_metadata(u, q, !self.casa_scalar_literals)
                    .map_err(|e| ExprParseError {
                        message: e.to_string(),
                        position: pos,
                    })
                    .map(|r| ExprNode::numeric(r, false))
            }
            "indexin" => {
                let axis = match arg1 {
                    ExprNode::Scalar(v) if (v.value - v.value.round()).abs() < 1e-9 => {
                        v.value.round() as i64
                    }
                    _ => {
                        return Err(ExprParseError {
                            message: "indexin() first argument must be an integer axis".into(),
                            position: pos,
                        });
                    }
                };
                let indices = match arg2 {
                    ExprNode::IntArray(vals) => vals,
                    ExprNode::Scalar(v) if (v.value - v.value.round()).abs() < 1e-9 => {
                        vec![v.value.round() as i64]
                    }
                    _ => {
                        return Err(ExprParseError {
                            message:
                                "indexin() second argument must be an integer or integer array"
                                    .into(),
                            position: pos,
                        });
                    }
                };
                Ok(ExprNode::Mask(MaskExprBuilder::index_in(axis, indices)))
            }
            "rebin" => {
                let single = arg1.single_precision();
                let expr = arg1.into_numeric(pos)?;
                let raw_factors = match arg2 {
                    ExprNode::IntArray(vals) => vals,
                    ExprNode::Scalar(v) if (v.value - v.value.round()).abs() < 1e-9 => {
                        vec![v.value.round() as i64]
                    }
                    _ => {
                        return Err(ExprParseError {
                            message: "rebin() second argument must be an integer factor vector"
                                .into(),
                            position: pos,
                        });
                    }
                };
                let factors: Vec<usize> = raw_factors
                    .into_iter()
                    .map(|f| {
                        if f >= 1 {
                            Ok(f as usize)
                        } else {
                            Err(ExprParseError {
                                message: "rebin() factors must be positive integers >= 1".into(),
                                position: pos,
                            })
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                expr.rebin(factors)
                    .map_err(|e| ExprParseError {
                        message: e.to_string(),
                        position: pos,
                    })
                    .map(|r| ExprNode::numeric(r, single))
            }
            _ => Err(ExprParseError {
                message: format!("unknown 2-argument function: {name}"),
                position: pos,
            }),
        }
    }

    fn eval_function_3(
        &self,
        name: &str,
        arg1: ExprNode<'a, T>,
        arg2: ExprNode<'a, T>,
        arg3: ExprNode<'a, T>,
        pos: usize,
    ) -> Result<ExprNode<'a, T>, ExprParseError> {
        let lower = name.to_ascii_lowercase();
        match lower.as_str() {
            "iif" => {
                // iif(condition, true_val, false_val): a scalar branch is
                // a constant expression beside an image one, as LEL allows.
                let condition = arg1.into_mask(pos)?;
                let single = arg2.single_precision() && arg3.single_precision();
                let if_true = arg2.into_numeric_or_constant(pos)?;
                let if_false = arg3.into_numeric_or_constant(pos)?;
                ImageExprBuilder::iif(condition, if_true, if_false)
                    .map_err(|e| ExprParseError {
                        message: e.to_string(),
                        position: pos,
                    })
                    .map(|r| ExprNode::numeric(r, single))
            }
            "fractilerange1d" | "fractilerange" => {
                // fractilerange1d(expr, frac1, frac2)
                let expr = arg1.into_numeric(pos)?;
                let frac1 = match arg2 {
                    ExprNode::Scalar(v) => v.value,
                    _ => {
                        return Err(ExprParseError {
                            message: "fractilerange1d() second argument must be a scalar".into(),
                            position: pos,
                        });
                    }
                };
                let frac2 = match arg3 {
                    ExprNode::Scalar(v) => v.value,
                    _ => {
                        return Err(ExprParseError {
                            message: "fractilerange1d() third argument must be a scalar".into(),
                            position: pos,
                        });
                    }
                };
                Ok(ExprNode::numeric(expr.fractile_range(frac1, frac2), false))
            }
            _ => Err(ExprParseError {
                message: format!("unknown 3-argument function: {name}"),
                position: pos,
            }),
        }
    }

    fn resolve_image(&self, name: &str, pos: usize) -> Result<ExprNode<'a, T>, ExprParseError> {
        let single = self
            .single_precision_sources
            .is_some_and(|source| source(name));
        if let Some(source) = self
            .complex_resolver
            .and_then(|resolver| resolver.resolve(name).ok())
        {
            return ImageExprBuilder::from_dyn(source)
                .map(|expr| ExprNode::Complex(Box::new(expr), single))
                .map_err(|error| ExprParseError {
                    message: error.to_string(),
                    position: pos,
                });
        }
        let image = self.resolver.resolve(name).map_err(|e| ExprParseError {
            message: e.to_string(),
            position: pos,
        })?;
        let expr = ImageExprBuilder::from_dyn(image).map_err(|e| ExprParseError {
            message: e.to_string(),
            position: pos,
        })?;
        Ok(ExprNode::numeric(expr, single))
    }

    fn parse_array_element(&mut self) -> Result<i64, ExprParseError> {
        let neg = if self.current == Token::Minus {
            self.advance()?;
            true
        } else {
            if self.current == Token::Plus {
                self.advance()?;
            }
            false
        };
        match self.current {
            Token::Number(value) if (value - value.round()).abs() < 1e-9 => {
                self.advance()?;
                let n = value.round() as i64;
                Ok(if neg { -n } else { n })
            }
            _ => Err(ExprParseError {
                message: format!(
                    "expected integer inside array literal, found {}",
                    self.current
                ),
                position: self.current_pos,
            }),
        }
    }

    fn parse_array_literal(&mut self) -> Result<ExprNode<'a, T>, ExprParseError> {
        self.expect(&Token::LBracket)?;
        let mut values = Vec::new();
        if self.current == Token::RBracket {
            self.advance()?;
            return Ok(ExprNode::IntArray(values));
        }
        loop {
            let start = self.parse_array_element()?;
            if self.current == Token::Colon {
                self.advance()?;
                let end = self.parse_array_element()?;
                if end < start {
                    return Err(ExprParseError {
                        message: format!("descending integer range {start}:{end} in array literal"),
                        position: self.current_pos,
                    });
                }
                values.extend(start..=end);
            } else {
                values.push(start);
            }
            if self.current == Token::Comma {
                self.advance()?;
                continue;
            }
            break;
        }
        self.expect(&Token::RBracket)?;
        Ok(ExprNode::IntArray(values))
    }

    fn parse_typed_array_literal(&mut self) -> Result<ExprNode<'a, T>, ExprParseError> {
        let pos = self.current_pos;
        self.expect(&Token::LBracket)?;
        let mut values = vec![self.parse_expression()?];
        while self.current == Token::Comma {
            self.advance()?;
            values.push(self.parse_expression()?);
        }
        self.expect(&Token::RBracket)?;
        let boolean = values
            .iter()
            .any(|value| matches!(value, ExprNode::Mask(_)));
        if boolean {
            if !values
                .iter()
                .all(|value| matches!(value, ExprNode::Mask(_)))
            {
                return Err(ExprParseError {
                    message:
                        "LatticeExprNode::resultDataType - Bool and numeric operands cannot mixed"
                            .into(),
                    position: pos,
                });
            }
            let flags = values
                .into_iter()
                .map(|value| {
                    let mask = value.into_mask(pos)?;
                    scalar_array_element(mask.shape(), pos)?;
                    mask.compile()
                        .and_then(|mask| mask.get_at(&[]))
                        .map_err(array_error(pos))
                })
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(ExprNode::Mask(MaskExprBuilder::from_constant(
                ndarray::Array1::from_vec(flags).into_dyn(),
            )));
        }
        let single = values.iter().all(ExprNode::single_precision);
        if values
            .iter()
            .any(|value| matches!(value, ExprNode::Complex(..)))
        {
            let pixels = values
                .into_iter()
                .map(|value| {
                    let expression = complex::promote(value, pos)?;
                    scalar_array_element(expression.shape(), pos)?;
                    expression
                        .compile()
                        .and_then(|expression| expression.get_at(&[]))
                        .map_err(array_error(pos))
                })
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(ExprNode::Complex(
                Box::new(ImageExprBuilder::from_vector(pixels)),
                single,
            ));
        }
        let pixels = values
            .into_iter()
            .map(|value| {
                let expression = value.into_numeric_or_constant(pos)?;
                scalar_array_element(expression.shape(), pos)?;
                expression
                    .compile()
                    .and_then(|expression| expression.get_at(&[]))
                    .map_err(array_error(pos))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ExprNode::numeric(
            ImageExprBuilder::from_vector(pixels),
            single,
        ))
    }

    fn parse_primary(&mut self) -> Result<ExprNode<'a, T>, ExprParseError> {
        match self.current.clone() {
            Token::LParen => {
                self.advance()?;
                let inner = self.parse_expression()?;
                self.expect(&Token::RParen)?;
                Ok(inner)
            }
            Token::LBracket if self.casa_scalar_literals && !self.integer_array_argument => {
                self.parse_typed_array_literal()
            }
            Token::LBracket => self.parse_array_literal(),
            Token::Number(value) => {
                let text = &self.lexer.src[self.current_pos..self.lexer.pos];
                let literal = if self.casa_scalar_literals {
                    if let Some((mantissa, _)) = text.split_once(['d', 'D']) {
                        // CASA recognizes a D exponent as a Double literal but its
                        // numeric conversion consumes only the leading mantissa.
                        ScalarLiteral::double(mantissa.parse().expect("valid numeric mantissa"))
                    } else if text.contains(['e', 'E']) || !text.contains('.') {
                        ScalarLiteral::single(value as f32)
                    } else {
                        ScalarLiteral::double(value)
                    }
                } else {
                    ScalarLiteral::double(value)
                };
                self.advance()?;
                Ok(ExprNode::Scalar(literal))
            }
            Token::QuotedPath(path) => {
                let pos = self.current_pos;
                self.advance()?;
                self.resolve_image(&path, pos)
            }
            _ => Err(ExprParseError {
                message: format!("unexpected token: {}", self.current),
                position: self.current_pos,
            }),
        }
    }
}

fn scalar_array_element(shape: &[usize], position: usize) -> Result<(), ExprParseError> {
    if shape.is_empty() {
        Ok(())
    } else {
        Err(ExprParseError {
            message: "array literal elements must be scalar".into(),
            position,
        })
    }
}

fn array_error(position: usize) -> impl FnOnce(ImageError) -> ExprParseError {
    move |error| ExprParseError {
        message: error.to_string(),
        position,
    }
}

fn flip_compare_op(op: ImageExprCompareOp) -> ImageExprCompareOp {
    match op {
        ImageExprCompareOp::GreaterThan => ImageExprCompareOp::LessThan,
        ImageExprCompareOp::LessThan => ImageExprCompareOp::GreaterThan,
        ImageExprCompareOp::GreaterEqual => ImageExprCompareOp::LessEqual,
        ImageExprCompareOp::LessEqual => ImageExprCompareOp::GreaterEqual,
        ImageExprCompareOp::Equal => ImageExprCompareOp::Equal,
        ImageExprCompareOp::NotEqual => ImageExprCompareOp::NotEqual,
    }
}

// ---------------------------------------------------------------------------
// Public entrypoints
// ---------------------------------------------------------------------------

/// Parses a LEL expression string and returns a numeric [`ImageExprBuilder`].
///
/// The expression must evaluate to a numeric (non-boolean) result. Image
/// references in the expression are resolved via the provided [`ImageResolver`].
///
/// # Errors
///
/// Returns [`ExprParseError`] for syntax errors, unknown function names,
/// unresolved image references, or type mismatches (e.g. a comparison
/// expression where a numeric result is expected).
///
/// # Example
///
/// ```rust,no_run
/// # use casa_images::expr_parser::*;
/// # use casa_images::PagedImage;
/// # use std::collections::HashMap;
/// let a = PagedImage::<f32>::open("a.image").unwrap();
/// let mut map = HashMap::new();
/// map.insert("a.image".to_string(), &a as &dyn casa_images::image::ImageInterface<f32>);
/// let resolver = HashMapResolver(map);
/// let expr = parse_image_expr("'a.image' * 2.0 + 1.0", &resolver).unwrap();
/// ```
fn parse_image_expr_builder<'a, T, R>(
    expr: &str,
    resolver: &R,
) -> Result<ImageExprBuilder<'a, T>, ExprParseError>
where
    T: ImageExprValue + PartialOrd + ExprValueConvert,
    R: ImageResolver<'a, T>,
{
    if expr.trim().is_empty() {
        return Err(ExprParseError {
            message: "empty expression".into(),
            position: 0,
        });
    }
    let mut parser = Parser::new(expr, resolver)?;
    let result = parser.parse_expression()?;
    if parser.current != Token::Eof {
        return Err(ExprParseError {
            message: format!("unexpected trailing token: {}", parser.current),
            position: parser.current_pos,
        });
    }
    let mut image_expr = result.into_numeric(0)?;
    image_expr.set_expr_string(expr);
    Ok(image_expr)
}

/// Parses and compiles a LEL numeric expression into the canonical owned
/// evaluator. Parsing and source capture happen exactly once in this call.
pub fn parse_image_expr<'a, T, R>(
    expr: &str,
    resolver: &R,
) -> Result<ImageExpression<T>, ExprParseError>
where
    T: ImageExprValue + PartialOrd + ExprValueConvert,
    R: ImageResolver<'a, T>,
{
    parse_image_expr_builder(expr, resolver)?
        .compile()
        .map_err(|error| ExprParseError {
            message: error.to_string(),
            position: 0,
        })
}

/// Parses a LEL expression string and returns a boolean [`MaskExprBuilder`].
///
/// The expression must evaluate to a boolean result (via comparison operators
/// and/or logical connectives). Image references are resolved via the
/// provided [`ImageResolver`].
///
/// # Errors
///
/// Returns [`ExprParseError`] if the expression is not boolean-valued, or
/// for any syntax/resolution error.
///
/// # Example
///
/// ```rust,no_run
/// # use casa_images::expr_parser::*;
/// # use casa_images::PagedImage;
/// # use std::collections::HashMap;
/// let a = PagedImage::<f32>::open("a.image").unwrap();
/// let mut map = HashMap::new();
/// map.insert("a.image".to_string(), &a as &dyn casa_images::image::ImageInterface<f32>);
/// let resolver = HashMapResolver(map);
/// let mask = parse_mask_expr("'a.image' > 0.5 && 'a.image' < 10.0", &resolver).unwrap();
/// ```
fn parse_mask_expr_builder<'a, T, R>(
    expr: &str,
    resolver: &R,
) -> Result<MaskExprBuilder<'a, T>, ExprParseError>
where
    T: ImageExprValue + PartialOrd + ExprValueConvert,
    R: ImageResolver<'a, T>,
{
    if expr.trim().is_empty() {
        return Err(ExprParseError {
            message: "empty expression".into(),
            position: 0,
        });
    }
    let mut parser = Parser::new(expr, resolver)?;
    let result = parser.parse_expression()?;
    if parser.current != Token::Eof {
        return Err(ExprParseError {
            message: format!("unexpected trailing token: {}", parser.current),
            position: parser.current_pos,
        });
    }
    result.into_mask(0)
}

pub(crate) fn subimage_mask_builder<'a, T, R>(
    expression: &str,
    real: &R,
    complex: &dyn ImageResolver<'a, Complex64>,
    single_precision: Option<&dyn Fn(&str) -> bool>,
) -> Result<MaskExprBuilder<'a, T>, ExprParseError>
where
    T: ImageExprValue + PartialOrd + ExprValueConvert,
    R: ImageResolver<'a, T>,
{
    let mut parser = Parser::new(expression, real)?;
    parser.complex_resolver = Some(complex);
    parser.casa_scalar_literals = single_precision.is_some();
    parser.check_lattice_geometry = single_precision.is_some();
    parser.single_precision_sources = single_precision;
    let result = parser.parse_expression()?;
    parser.expect(&Token::Eof)?;
    Ok(match result {
        ExprNode::Numeric(image, _) if !image.shape().is_empty() => {
            image.compare_scalar(T::from_f64(0.5), ImageExprCompareOp::GreaterEqual)
        }
        other => other.into_mask(0)?,
    })
}

enum CasaExpressionMode {
    Fill,
    PreserveValidity,
    SectionMask,
}

fn parse_casa_pixel_expression<'a, R>(
    expression: &str,
    real: &R,
    complex: &dyn ImageResolver<'a, Complex64>,
    single_precision: &dyn Fn(&str) -> bool,
    mode: CasaExpressionMode,
) -> Result<ExprNode<'a, f64>, ExprParseError>
where
    R: ImageResolver<'a, f64>,
{
    let mut parser = Parser::new(expression, real)?;
    parser.complex_resolver = Some(complex);
    parser.casa_scalar_literals = true;
    parser.fill_empty_reductions = !matches!(mode, CasaExpressionMode::PreserveValidity);
    parser.check_lattice_geometry = matches!(mode, CasaExpressionMode::SectionMask);
    parser.single_precision_sources = Some(single_precision);
    let result = parser.parse_expression()?;
    parser.expect(&Token::Eof)?;
    Ok(result)
}

pub(crate) fn parse_image_tool_mask<'a, R>(
    expression: &str,
    real: &R,
    complex: &dyn ImageResolver<'a, Complex64>,
    single_precision: &dyn Fn(&str) -> bool,
) -> Result<MaskExpression<f64>, ImageError>
where
    R: ImageResolver<'a, f64>,
{
    image_tool_mask_builder(expression, real, complex, single_precision, false)?.compile()
}

pub(crate) fn image_tool_mask_builder<'a, R>(
    expression: &str,
    real: &R,
    complex: &dyn ImageResolver<'a, Complex64>,
    single_precision: &dyn Fn(&str) -> bool,
    section: bool,
) -> Result<MaskExprBuilder<'a, f64>, ImageError>
where
    R: ImageResolver<'a, f64>,
{
    let result = parse_casa_pixel_expression(
        expression,
        real,
        complex,
        single_precision,
        if section {
            CasaExpressionMode::SectionMask
        } else {
            CasaExpressionMode::Fill
        },
    )
    .map_err(|error| ImageError::CasaSelection(crate::analysis::mask_error_wording(error)))?;
    let ExprNode::Mask(mask) = result else {
        return Err(ImageError::CasaSelection(
            "Exception: The expression type must be Boolean.".into(),
        ));
    };
    Ok(mask)
}

/// The evaluated pixel type of a public image calculator expression.
pub enum ImageToolPixelExpression {
    /// Real pixels, with double precision used for promoted operands.
    Real(ImageExpression<f64>),
    /// Complex pixels, with double precision used for promoted operands.
    Complex(ImageExpression<Complex64>),
    /// Boolean result, with its lattice shape when it has one.
    Boolean(Vec<usize>),
}

pub(crate) fn compile_image_tool_pixels<'a, R>(
    expression: &str,
    real: &R,
    complex: &dyn ImageResolver<'a, Complex64>,
    single_precision: &dyn Fn(&str) -> bool,
) -> Result<(ImageToolPixelExpression, bool), ImageError>
where
    R: ImageResolver<'a, f64>,
{
    let parsed = parse_casa_pixel_expression(
        expression,
        real,
        complex,
        single_precision,
        CasaExpressionMode::Fill,
    )
    .map_err(|error| ImageError::CasaSelection(crate::analysis::mask_error_wording(error)))?;
    let single_precision = parsed.single_precision();
    let expression = match parsed {
        ExprNode::Scalar(scalar) => ImageExprBuilder::scalar(scalar.value)
            .compile()
            .map(ImageToolPixelExpression::Real),
        ExprNode::Numeric(builder, _) => builder.compile().map(ImageToolPixelExpression::Real),
        ExprNode::Complex(builder, _) => builder.compile().map(ImageToolPixelExpression::Complex),
        ExprNode::IntArray(values) => {
            ImageExprBuilder::from_vector(values.into_iter().map(|value| value as f64).collect())
                .compile()
                .map(ImageToolPixelExpression::Real)
        }
        ExprNode::Mask(mask) => Ok(ImageToolPixelExpression::Boolean(mask.shape().to_vec())),
    }?;
    Ok((expression, single_precision))
}

pub(crate) enum ReplacementPixels {
    Real(ndarray::ArrayD<f64>),
    Complex(ndarray::ArrayD<Complex64>),
}

pub(crate) fn evaluate_replacement<'a, R>(
    expression: &str,
    real: &R,
    complex: &dyn ImageResolver<'a, Complex64>,
    single_precision: &dyn Fn(&str) -> bool,
    selected_shape: &[usize],
    want_complex: bool,
) -> Result<ReplacementPixels, ImageError>
where
    R: ImageResolver<'a, f64>,
{
    let check_shape = |shape: &[usize]| {
        if shape.is_empty() || shape == selected_shape {
            Ok(())
        } else {
            Err(ImageError::CasaSelection(
                "LELAttribute: shapes of operands mismatch".into(),
            ))
        }
    };
    if let Some(vector) = expression
        .trim()
        .strip_prefix('[')
        .and_then(|text| text.strip_suffix(']'))
        && let Ok(values) = vector
            .split(',')
            .map(|item| item.trim().parse::<f64>())
            .collect::<Result<Vec<_>, _>>()
    {
        check_shape(&[values.len()])?;
        return Ok(ReplacementPixels::Real(
            ndarray::Array1::from_vec(values).into_dyn(),
        ));
    }
    let result = parse_casa_pixel_expression(
        expression,
        real,
        complex,
        single_precision,
        CasaExpressionMode::Fill,
    )?;
    match result {
        ExprNode::Scalar(literal) => Ok(ReplacementPixels::Real(
            ndarray::arr0(literal.value).into_dyn(),
        )),
        ExprNode::Numeric(builder, _) => {
            check_shape(builder.shape())?;
            Ok(ReplacementPixels::Real(builder.compile()?.get()?))
        }
        ExprNode::Complex(builder, _) if want_complex => {
            check_shape(builder.shape())?;
            Ok(ReplacementPixels::Complex(builder.compile()?.get()?))
        }
        ExprNode::IntArray(values) => {
            check_shape(&[values.len()])?;
            let values = ndarray::Array1::from_iter(values.into_iter().map(|value| value as f64));
            Ok(ReplacementPixels::Real(values.into_dyn()))
        }
        _ => Err(ImageError::CasaSelection(format!(
            "LatticeExprNode::make{} - conversion to {} not possible",
            if want_complex { "Complex" } else { "Float" },
            if want_complex { "Complex" } else { "Float" },
        ))),
    }
}

pub(crate) fn evaluate_real_scalar<'a, R>(
    expression: &str,
    real: &R,
    complex: &dyn ImageResolver<'a, Complex64>,
    single_precision: &dyn Fn(&str) -> bool,
) -> Result<f64, ImageError>
where
    R: ImageResolver<'a, f64>,
{
    // Preserve scalar validity so set can reject a reduction with no valid samples.
    let result = parse_casa_pixel_expression(
        expression,
        real,
        complex,
        single_precision,
        CasaExpressionMode::PreserveValidity,
    )?;
    let cannot_convert = || {
        ImageError::CasaSelection(
            "LatticeExprNode::makeFloat - conversion to Float not possible".into(),
        )
    };
    match result {
        ExprNode::Scalar(value) => Ok(value.value),
        ExprNode::Numeric(builder, _) if builder.shape().is_empty() => {
            let compiled = builder.compile()?;
            if compiled
                .get_mask_slice(&[], &[])?
                .is_some_and(|mask| !mask[ndarray::IxDyn(&[])])
            {
                return Err(ImageError::CasaSelection(
                    "The scalar pixels expression is invalid".into(),
                ));
            }
            compiled.get_at(&[])
        }
        ExprNode::Mask(builder) if builder.shape().is_empty() => Err(cannot_convert()),
        ExprNode::Complex(builder, _) if builder.shape().is_empty() => Err(cannot_convert()),
        _ => Err(ImageError::CasaSelection(
            "The pixels expression must be scalar".into(),
        )),
    }
}

/// Parses and compiles a LEL mask expression into the canonical owned
/// evaluator. Parsing and source capture happen exactly once in this call.
pub fn parse_mask_expr<'a, T, R>(
    expr: &str,
    resolver: &R,
) -> Result<MaskExpression<T>, ExprParseError>
where
    T: ImageExprValue + PartialOrd + ExprValueConvert,
    R: ImageResolver<'a, T>,
{
    parse_mask_expr_builder(expr, resolver)?
        .compile()
        .map_err(|error| ExprParseError {
            message: error.to_string(),
            position: 0,
        })
}

// ---------------------------------------------------------------------------
// ImageExprValue extension: from_f64 / to_f64
// ---------------------------------------------------------------------------

/// Extension for converting between `f64` and pixel types during parsing.
///
/// This is used internally by the parser to handle numeric literal constants.
/// It is intentionally narrower than a full `From<f64>` implementation to
/// avoid surprising precision behavior.
/// Conversion between pixel values and `f64` for constant folding during
/// expression parsing.
///
/// Implemented for `f32` and `f64`.  The parser folds purely-scalar
/// sub-expressions (e.g. `2.0 + 3.0`) at parse time to avoid building
/// unnecessary DAG nodes.
pub trait ExprValueConvert: Sized {
    /// Create a value from an `f64` literal (potentially lossy for `f32`).
    fn from_f64(v: f64) -> Self;
    /// Convert to `f64` for constant-folding during parse.
    fn to_f64(self) -> f64;
    /// Whether this pixel type is complex-valued.
    fn is_complex() -> bool;
}

impl ExprValueConvert for f32 {
    fn from_f64(v: f64) -> Self {
        v as f32
    }
    fn to_f64(self) -> f64 {
        self as f64
    }
    fn is_complex() -> bool {
        false
    }
}

impl ExprValueConvert for f64 {
    fn from_f64(v: f64) -> Self {
        v
    }
    fn to_f64(self) -> f64 {
        self
    }
    fn is_complex() -> bool {
        false
    }
}

impl ExprValueConvert for casa_types::Complex32 {
    fn from_f64(v: f64) -> Self {
        Self::new(v as f32, 0.0)
    }
    fn to_f64(self) -> f64 {
        self.re as f64
    }
    fn is_complex() -> bool {
        true
    }
}

impl ExprValueConvert for casa_types::Complex64 {
    fn from_f64(v: f64) -> Self {
        Self::new(v, 0.0)
    }
    fn to_f64(self) -> f64 {
        self.re
    }
    fn is_complex() -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::temp_image::TempImage;
    use casa_coordinates::CoordinateSystem;

    fn make_test_image(shape: Vec<usize>, value: f32) -> TempImage<f32> {
        let mut img = TempImage::<f32>::new(
            shape,
            CoordinateSystem::new(),
            casa_lattices::TempStoragePolicy::Memory,
        )
        .unwrap();
        img.set(value).unwrap();
        img
    }

    /// Build a resolver from image-name / image-ref pairs.
    /// Callers must keep the `TempImage` values alive for the resolver's
    /// lifetime, so we accept already-populated HashMap content.
    fn make_resolver<'a>(pairs: &[(&str, &'a TempImage<f32>)]) -> HashMapResolver<'a, f32> {
        let mut map = HashMap::new();
        for &(name, img) in pairs {
            map.insert(name.to_string(), img as &dyn ImageInterface<f32>);
        }
        HashMapResolver(map)
    }

    // -- Lexer tests --

    #[test]
    fn lex_basic_tokens() {
        let mut lex = Lexer::new("+ - * / ^ ( ) , == != > >= < <= && || !");
        let tokens: Vec<Token> = std::iter::from_fn(|| {
            let (tok, _) = lex.next_token().ok()?;
            if tok == Token::Eof { None } else { Some(tok) }
        })
        .collect();
        assert_eq!(
            tokens,
            vec![
                Token::Plus,
                Token::Minus,
                Token::Star,
                Token::Slash,
                Token::Caret,
                Token::LParen,
                Token::RParen,
                Token::Comma,
                Token::Eq,
                Token::Ne,
                Token::Gt,
                Token::Ge,
                Token::Lt,
                Token::Le,
                Token::And,
                Token::Or,
                Token::Bang,
            ]
        );
    }

    #[test]
    fn lex_number_literals() {
        let cases = [
            ("42", 42.0),
            ("3.15", 3.15),
            (".5", 0.5),
            ("1e3", 1000.0),
            ("2.5E-1", 0.25),
            ("1.5d2", 150.0),
        ];
        for (input, expected) in cases {
            let mut lex = Lexer::new(input);
            let (tok, _) = lex.next_token().unwrap();
            if let Token::Number(v) = tok {
                assert!(
                    (v - expected).abs() < 1e-10,
                    "input={input}: got {v}, expected {expected}"
                );
            } else {
                panic!("expected Number for input={input}, got {tok:?}");
            }
        }
    }

    #[test]
    fn lex_quoted_paths() {
        let mut lex = Lexer::new("'hello' \"world\"");
        let (t1, _) = lex.next_token().unwrap();
        let (t2, _) = lex.next_token().unwrap();
        assert_eq!(t1, Token::QuotedPath("hello".into()));
        assert_eq!(t2, Token::QuotedPath("world".into()));
    }

    #[test]
    fn lex_unterminated_string() {
        let mut lex = Lexer::new("'abc");
        let result = lex.next_token();
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("unterminated"));
    }

    #[test]
    fn lex_bare_ident_with_dots() {
        let mut lex = Lexer::new("my_image.im");
        let (tok, _) = lex.next_token().unwrap();
        assert_eq!(tok, Token::Ident("my_image.im".into()));
    }

    // -- Parser tests --

    #[test]
    fn parse_simple_scalar_add() {
        let a = make_test_image(vec![2, 2], 3.0);
        let r = make_resolver(&[("a", &a)]);
        let expr = parse_image_expr("'a' + 1.0", &r).unwrap();
        assert_eq!(expr.get_at(&[0, 0]).unwrap(), 4.0);
    }

    #[test]
    fn parse_arithmetic_precedence() {
        // 2.0 + 3.0 * a  with a=4.0 should be 2.0 + 12.0 = 14.0
        let a = make_test_image(vec![2, 2], 4.0);
        let r = make_resolver(&[("a", &a)]);
        let expr = parse_image_expr("2.0 + 3.0 * 'a'", &r).unwrap();
        assert_eq!(expr.get_at(&[0, 0]).unwrap(), 14.0);
    }

    #[test]
    fn parse_power_right_associative() {
        // 2.0 ^ 3.0 ^ 2.0 = 2.0 ^ 9.0 = 512.0 (right-associative)
        let a = make_test_image(vec![2, 2], 1.0);
        let r = make_resolver(&[("a", &a)]);
        // Use image to anchor shape: a * 0 + (2^3^2)
        let expr = parse_image_expr("'a' * 0 + 2.0 ^ 3.0 ^ 2.0", &r).unwrap();
        assert_eq!(expr.get_at(&[0, 0]).unwrap(), 512.0);
    }

    #[test]
    fn parse_unary_negate() {
        let a = make_test_image(vec![2, 2], 5.0);
        let r = make_resolver(&[("a", &a)]);
        let expr = parse_image_expr("-'a'", &r).unwrap();
        assert_eq!(expr.get_at(&[0, 0]).unwrap(), -5.0);
    }

    #[test]
    fn parse_function_sin() {
        let a = make_test_image(vec![2, 2], 1.0);
        let r = make_resolver(&[("a", &a)]);
        let expr = parse_image_expr("sin('a')", &r).unwrap();
        let expected = 1.0_f32.sin();
        assert!((expr.get_at(&[0, 0]).unwrap() - expected).abs() < 1e-6);
    }

    #[test]
    fn parse_function_pow_2arg() {
        let a = make_test_image(vec![2, 2], 3.0);
        let r = make_resolver(&[("a", &a)]);
        let expr = parse_image_expr("pow('a', 2.0)", &r).unwrap();
        assert_eq!(expr.get_at(&[0, 0]).unwrap(), 9.0);
    }

    #[test]
    fn parse_constants_pi_e() {
        let a = make_test_image(vec![2, 2], 1.0);
        let r = make_resolver(&[("a", &a)]);
        let expr = parse_image_expr("'a' * pi()", &r).unwrap();
        let expected = std::f64::consts::PI as f32;
        assert!((expr.get_at(&[0, 0]).unwrap() - expected).abs() < 1e-5);
    }

    #[test]
    fn parse_parenthesized_expression() {
        // (a + 1.0) * 2.0  with a=3.0 => 8.0
        let a = make_test_image(vec![2, 2], 3.0);
        let r = make_resolver(&[("a", &a)]);
        let expr = parse_image_expr("('a' + 1.0) * 2.0", &r).unwrap();
        assert_eq!(expr.get_at(&[0, 0]).unwrap(), 8.0);
    }

    #[test]
    fn parse_complex_expression() {
        // sqrt(a^2 + 1.0) with a=3.0 => sqrt(10.0)
        let a = make_test_image(vec![2, 2], 3.0);
        let r = make_resolver(&[("a", &a)]);
        let expr = parse_image_expr("sqrt('a' ^ 2.0 + 1.0)", &r).unwrap();
        let expected = 10.0_f32.sqrt();
        assert!((expr.get_at(&[0, 0]).unwrap() - expected).abs() < 1e-5);
    }

    #[test]
    fn parse_two_images() {
        let a = make_test_image(vec![2, 2], 3.0);
        let b = make_test_image(vec![2, 2], 4.0);
        let r = make_resolver(&[("a", &a), ("b", &b)]);
        let expr = parse_image_expr("'a' + 'b'", &r).unwrap();
        assert_eq!(expr.get_at(&[0, 0]).unwrap(), 7.0);
    }

    #[test]
    fn parse_scalar_minus_image() {
        // 10.0 - a with a=3.0 => 7.0
        let a = make_test_image(vec![2, 2], 3.0);
        let r = make_resolver(&[("a", &a)]);
        let expr = parse_image_expr("10.0 - 'a'", &r).unwrap();
        assert_eq!(expr.get_at(&[0, 0]).unwrap(), 7.0);
    }

    #[test]
    fn parse_scalar_divide_image() {
        // 12.0 / a with a=3.0 => 4.0
        let a = make_test_image(vec![2, 2], 3.0);
        let r = make_resolver(&[("a", &a)]);
        let expr = parse_image_expr("12.0 / 'a'", &r).unwrap();
        assert_eq!(expr.get_at(&[0, 0]).unwrap(), 4.0);
    }

    #[test]
    fn parse_mask_comparison() {
        let a = make_test_image(vec![2, 2], 5.0);
        let r = make_resolver(&[("a", &a)]);
        let mask = parse_mask_expr("'a' > 3.0", &r).unwrap();
        assert!(mask.get_at(&[0, 0]).unwrap());
    }

    #[test]
    fn parse_mask_and_or() {
        let a = make_test_image(vec![2, 2], 5.0);
        let r = make_resolver(&[("a", &a)]);
        let mask = parse_mask_expr("'a' > 3.0 && 'a' < 10.0", &r).unwrap();
        assert!(mask.get_at(&[0, 0]).unwrap());

        let mask2 = parse_mask_expr("'a' < 3.0 || 'a' > 4.0", &r).unwrap();
        assert!(mask2.get_at(&[0, 0]).unwrap());
    }

    #[test]
    fn parse_mask_not() {
        let a = make_test_image(vec![2, 2], 5.0);
        let r = make_resolver(&[("a", &a)]);
        let mask = parse_mask_expr("!('a' < 3.0)", &r).unwrap();
        assert!(mask.get_at(&[0, 0]).unwrap());
    }

    #[test]
    fn parse_bare_ident_image() {
        let a = make_test_image(vec![2, 2], 7.0);
        let r = make_resolver(&[("myimg", &a)]);
        let expr = parse_image_expr("myimg + 1.0", &r).unwrap();
        assert_eq!(expr.get_at(&[0, 0]).unwrap(), 8.0);
    }

    #[test]
    fn parse_double_quoted_path() {
        let a = make_test_image(vec![2, 2], 2.0);
        let r = make_resolver(&[("test.image", &a)]);
        let expr = parse_image_expr("\"test.image\" * 3.0", &r).unwrap();
        assert_eq!(expr.get_at(&[0, 0]).unwrap(), 6.0);
    }

    #[test]
    fn parse_case_insensitive_functions() {
        let a = make_test_image(vec![2, 2], 1.0);
        let r = make_resolver(&[("a", &a)]);
        let expr = parse_image_expr("SIN('a')", &r).unwrap();
        let expected = 1.0_f32.sin();
        assert!((expr.get_at(&[0, 0]).unwrap() - expected).abs() < 1e-6);
    }

    #[test]
    fn parse_nested_functions() {
        // sqrt(abs(-4.0 + a)) with a=0 => sqrt(4.0) = 2.0
        let a = make_test_image(vec![2, 2], 0.0);
        let r = make_resolver(&[("a", &a)]);
        let expr = parse_image_expr("sqrt(abs(-4.0 + 'a'))", &r).unwrap();
        assert!((expr.get_at(&[0, 0]).unwrap() - 2.0).abs() < 1e-6);
    }

    #[test]
    fn parse_min_max_functions() {
        let a = make_test_image(vec![2, 2], 5.0);
        let r = make_resolver(&[("a", &a)]);
        let expr = parse_image_expr("min('a', 3.0)", &r).unwrap();
        assert_eq!(expr.get_at(&[0, 0]).unwrap(), 3.0);

        let expr2 = parse_image_expr("max('a', 7.0)", &r).unwrap();
        assert_eq!(expr2.get_at(&[0, 0]).unwrap(), 7.0);
    }

    // -- Negative tests --

    #[test]
    fn parse_empty_expression() {
        let r: HashMapResolver<f32> = HashMapResolver(HashMap::new());
        let err = parse_image_expr("", &r).unwrap_err();
        assert!(err.message.contains("empty"));
    }

    #[test]
    fn parse_whitespace_only() {
        let r: HashMapResolver<f32> = HashMapResolver(HashMap::new());
        let err = parse_image_expr("   ", &r).unwrap_err();
        assert!(err.message.contains("empty"));
    }

    #[test]
    fn parse_unknown_image() {
        let r: HashMapResolver<f32> = HashMapResolver(HashMap::new());
        let err = parse_image_expr("'nonexistent.image'", &r).unwrap_err();
        assert!(err.message.contains("unknown image"));
    }

    #[test]
    fn parse_unknown_function() {
        let a = make_test_image(vec![2, 2], 1.0);
        let r = make_resolver(&[("a", &a)]);
        let err = parse_image_expr("bogus('a')", &r).unwrap_err();
        assert!(err.message.contains("unknown"));
    }

    #[test]
    fn parse_unterminated_quote() {
        let r: HashMapResolver<f32> = HashMapResolver(HashMap::new());
        let err = parse_image_expr("'abc", &r).unwrap_err();
        assert!(err.message.contains("unterminated"));
    }

    #[test]
    fn parse_mismatched_parens() {
        let a = make_test_image(vec![2, 2], 1.0);
        let r = make_resolver(&[("a", &a)]);
        let err = parse_image_expr("('a' + 1.0", &r).unwrap_err();
        assert!(
            err.message.contains("expected )") || err.message.contains("expected ("),
            "got: {}",
            err.message
        );
    }

    #[test]
    fn parse_trailing_operator() {
        let a = make_test_image(vec![2, 2], 1.0);
        let r = make_resolver(&[("a", &a)]);
        let err = parse_image_expr("'a' +", &r).unwrap_err();
        assert!(!err.message.is_empty());
    }

    #[test]
    fn parse_numeric_where_mask_expected() {
        let a = make_test_image(vec![2, 2], 1.0);
        let r = make_resolver(&[("a", &a)]);
        let err = parse_mask_expr("'a' + 1.0", &r).unwrap_err();
        assert!(err.message.contains("boolean") || err.message.contains("mask"));
    }

    #[test]
    fn parse_mask_where_numeric_expected() {
        let a = make_test_image(vec![2, 2], 1.0);
        let r = make_resolver(&[("a", &a)]);
        let err = parse_image_expr("'a' > 1.0", &r).unwrap_err();
        assert!(err.message.contains("boolean") || err.message.contains("mask"));
    }

    #[test]
    fn parse_lone_equals_error() {
        let a = make_test_image(vec![2, 2], 1.0);
        let r = make_resolver(&[("a", &a)]);
        let err = parse_image_expr("'a' = 1.0", &r).unwrap_err();
        assert!(err.message.contains("=="));
    }

    #[test]
    fn parse_flipped_comparison() {
        // 3.0 < a with a=5.0 => true (parsed as a > 3.0)
        let a = make_test_image(vec![2, 2], 5.0);
        let r = make_resolver(&[("a", &a)]);
        let mask = parse_mask_expr("3.0 < 'a'", &r).unwrap();
        assert!(mask.get_at(&[0, 0]).unwrap());
    }

    #[test]
    fn scalar_minus_inf_preserves_inf() {
        // 10.0 - img where img contains Inf should yield -Inf, not NaN.
        let mut a = make_test_image(vec![2, 2], 0.0);
        a.put_at(f32::INFINITY, &[0, 0]).unwrap();
        let r = make_resolver(&[("a", &a)]);
        let expr = parse_image_expr("10.0 - 'a'", &r).unwrap();
        let val = expr.get_at(&[0, 0]).unwrap();
        assert!(val.is_infinite() && val < 0.0, "expected -Inf, got {val}");
    }

    #[test]
    fn scalar_divide_inf_preserves_zero() {
        // 1.0 / img where img contains Inf should yield 0, not NaN.
        let mut a = make_test_image(vec![2, 2], 0.0);
        a.put_at(f32::INFINITY, &[0, 0]).unwrap();
        let r = make_resolver(&[("a", &a)]);
        let expr = parse_image_expr("1.0 / 'a'", &r).unwrap();
        let val = expr.get_at(&[0, 0]).unwrap();
        assert_eq!(val, 0.0, "expected 0.0, got {val}");
    }

    // ========================================================================
    // Wave 14 parser tests
    // ========================================================================

    #[test]
    fn parse_isnan() {
        let mut a = make_test_image(vec![2, 2], 1.0);
        a.put_at(f32::NAN, &[0, 0]).unwrap();
        let r = make_resolver(&[("a", &a)]);
        let mask = parse_mask_expr("isnan('a')", &r).unwrap();
        assert!(mask.get_at(&[0, 0]).unwrap());
        assert!(!mask.get_at(&[0, 1]).unwrap());
    }

    #[test]
    fn parse_ndim() {
        // ndim returns a scalar; use in combined expression to get image shape
        let a = make_test_image(vec![4, 5, 6], 1.0);
        let r = make_resolver(&[("a", &a)]);
        let expr = parse_image_expr("'a' * 0.0 + ndim('a')", &r).unwrap();
        assert!((expr.get_at(&[0, 0, 0]).unwrap() - 3.0).abs() < 1e-5);
    }

    #[test]
    fn parse_nelements() {
        let a = make_test_image(vec![4, 5], 1.0);
        let r = make_resolver(&[("a", &a)]);
        let expr = parse_image_expr("'a' * 0.0 + nelements('a')", &r).unwrap();
        assert!((expr.get_at(&[0, 0]).unwrap() - 20.0).abs() < 1e-5);
    }

    #[test]
    fn parse_length() {
        let a = make_test_image(vec![4, 7], 1.0);
        let r = make_resolver(&[("a", &a)]);
        let expr = parse_image_expr("'a' * 0.0 + length('a', 1)", &r).unwrap();
        assert!((expr.get_at(&[0, 0]).unwrap() - 7.0).abs() < 1e-5);
    }

    /// Extract scalar from a 0-D reduction result.
    fn scalar_of(expr: &ImageExpression<f32>) -> f32 {
        assert!(
            expr.shape().is_empty(),
            "expected 0-D reduction, got {:?}",
            expr.shape()
        );
        expr.get().unwrap()[ndarray::IxDyn(&[])]
    }

    #[test]
    fn parse_sum() {
        let mut a = make_test_image(vec![2, 2], 0.0);
        a.put_slice(
            &ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[2, 2]), vec![1.0, 2.0, 3.0, 4.0])
                .unwrap(),
            &[0, 0],
        )
        .unwrap();
        let r = make_resolver(&[("a", &a)]);
        let expr = parse_image_expr("sum('a')", &r).unwrap();
        assert!(expr.shape().is_empty(), "sum should produce 0-D");
        assert!((scalar_of(&expr) - 10.0).abs() < 1e-5);
    }

    #[test]
    fn parse_min1d_max1d() {
        let mut a = make_test_image(vec![4], 0.0);
        a.put_slice(
            &ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[4]), vec![3.0, 1.0, 4.0, 2.0])
                .unwrap(),
            &[0],
        )
        .unwrap();
        let r = make_resolver(&[("a", &a)]);
        let min_expr = parse_image_expr("min1d('a')", &r).unwrap();
        let max_expr = parse_image_expr("max1d('a')", &r).unwrap();
        assert!((scalar_of(&min_expr) - 1.0).abs() < 1e-5);
        assert!((scalar_of(&max_expr) - 4.0).abs() < 1e-5);
    }

    #[test]
    fn parse_mean1d() {
        let mut a = make_test_image(vec![4], 0.0);
        a.put_slice(
            &ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[4]), vec![1.0, 2.0, 3.0, 4.0])
                .unwrap(),
            &[0],
        )
        .unwrap();
        let r = make_resolver(&[("a", &a)]);
        let expr = parse_image_expr("mean1d('a')", &r).unwrap();
        assert!((scalar_of(&expr) - 2.5).abs() < 1e-5);
    }

    #[test]
    fn parse_median1d() {
        let mut a = make_test_image(vec![5], 0.0);
        a.put_slice(
            &ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[5]), vec![5.0, 1.0, 3.0, 4.0, 2.0])
                .unwrap(),
            &[0],
        )
        .unwrap();
        let r = make_resolver(&[("a", &a)]);
        let expr = parse_image_expr("median1d('a')", &r).unwrap();
        assert!((scalar_of(&expr) - 3.0).abs() < 1e-5);
    }

    #[test]
    fn parse_fractile1d() {
        let mut a = make_test_image(vec![5], 0.0);
        a.put_slice(
            &ndarray::ArrayD::from_shape_vec(
                ndarray::IxDyn(&[5]),
                vec![10.0, 20.0, 30.0, 40.0, 50.0],
            )
            .unwrap(),
            &[0],
        )
        .unwrap();
        let r = make_resolver(&[("a", &a)]);
        let expr = parse_image_expr("fractile1d('a', 0.5)", &r).unwrap();
        assert!((scalar_of(&expr) - 30.0).abs() < 1e-5);
    }

    #[test]
    fn parse_fractilerange1d_three_args() {
        let mut a = make_test_image(vec![5], 0.0);
        a.put_slice(
            &ndarray::ArrayD::from_shape_vec(
                ndarray::IxDyn(&[5]),
                vec![10.0, 20.0, 30.0, 40.0, 50.0],
            )
            .unwrap(),
            &[0],
        )
        .unwrap();
        let r = make_resolver(&[("a", &a)]);
        let expr = parse_image_expr("fractilerange1d('a', 0.25, 0.75)", &r).unwrap();
        let val = scalar_of(&expr);
        // 20.0 and 40.0 → range = 20.0
        assert!((val - 20.0).abs() < 1e-5, "got {val}");
    }

    #[test]
    fn parse_iif() {
        let mut a = make_test_image(vec![4], 0.0);
        a.put_slice(
            &ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[4]), vec![1.0, 5.0, 3.0, 7.0])
                .unwrap(),
            &[0],
        )
        .unwrap();
        let r = make_resolver(&[("a", &a)]);
        let expr = parse_image_expr("iif('a' > 4.0, 'a' + 100.0, 'a')", &r).unwrap();
        let result = expr.get().unwrap();
        let expected =
            ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[4]), vec![1.0, 105.0, 3.0, 107.0])
                .unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn parse_ntrue_nfalse() {
        let mut a = make_test_image(vec![4], 0.0);
        a.put_slice(
            &ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[4]), vec![1.0, 5.0, 3.0, 7.0])
                .unwrap(),
            &[0],
        )
        .unwrap();
        let r = make_resolver(&[("a", &a)]);
        let nt = parse_image_expr("ntrue('a' > 4.0)", &r).unwrap();
        let nf = parse_image_expr("nfalse('a' > 4.0)", &r).unwrap();
        assert!(nt.shape().is_empty(), "ntrue should produce 0-D");
        assert!((scalar_of(&nt) - 2.0).abs() < 1e-5);
        assert!((scalar_of(&nf) - 2.0).abs() < 1e-5);
    }

    #[test]
    fn parse_all_any() {
        let a = make_test_image(vec![4], 3.0);
        let r = make_resolver(&[("a", &a)]);
        let all_mask = parse_mask_expr("all('a' > 2.0)", &r).unwrap();
        assert!(all_mask.shape().is_empty(), "all() should produce 0-D");
        assert!(all_mask.get().unwrap()[ndarray::IxDyn(&[])]);
        let any_mask = parse_mask_expr("any('a' > 5.0)", &r).unwrap();
        assert!(!any_mask.get().unwrap()[ndarray::IxDyn(&[])]);
    }

    #[test]
    fn parse_value_is_identity() {
        let a = make_test_image(vec![2, 2], 7.0);
        let r = make_resolver(&[("a", &a)]);
        let expr = parse_image_expr("value('a')", &r).unwrap();
        assert!((expr.get_at(&[0, 0]).unwrap() - 7.0).abs() < 1e-5);
    }

    #[test]
    fn parse_mask_and_replace_propagate_masked_derived_source() {
        let mut a = make_test_image(vec![4], 0.0);
        a.put_slice(
            &ndarray::ArrayD::from_shape_vec(
                ndarray::IxDyn(&[4]),
                vec![1.0, f32::INFINITY, 3.0, 4.0],
            )
            .unwrap(),
            &[0],
        )
        .unwrap();
        a.make_mask("quality", true, true).unwrap();
        let mask =
            ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[4]), vec![true, false, true, true])
                .unwrap();
        a.put_mask("quality", &mask).unwrap();
        a.set_default_mask("quality").unwrap();

        let r = make_resolver(&[("a", &a)]);

        let mask_expr = parse_mask_expr("mask('a' + 1.0)", &r).unwrap();
        assert_eq!(mask_expr.get().unwrap(), mask);

        let replaced = parse_image_expr("replace('a' + 1.0, 42.0)", &r).unwrap();
        let expected =
            ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[4]), vec![2.0, 42.0, 4.0, 5.0])
                .unwrap();
        assert_eq!(replaced.get().unwrap(), expected);
    }

    #[test]
    fn parse_real_imag_error_for_real_types() {
        let a = make_test_image(vec![2, 2], -3.0);
        let r = make_resolver(&[("a", &a)]);
        let real_expr = parse_image_expr("real('a')", &r).unwrap();
        assert!((real_expr.get_at(&[0, 0]).unwrap() + 3.0).abs() < 1e-5);
        let float_expr = parse_image_expr("float('a')", &r).unwrap();
        assert!((float_expr.get_at(&[0, 0]).unwrap() + 3.0).abs() < 1e-5);
        let double_expr = parse_image_expr("double('a')", &r).unwrap();
        assert!((double_expr.get_at(&[0, 0]).unwrap() + 3.0).abs() < 1e-5);
        let amp_expr = parse_image_expr("amplitude('a')", &r).unwrap();
        assert!((amp_expr.get_at(&[0, 0]).unwrap() - 3.0).abs() < 1e-5);

        let err = parse_image_expr("imag('a')", &r).unwrap_err();
        assert!(
            err.message.contains("typed API") || err.message.contains("type-changing"),
            "got: {}",
            err.message
        );
    }

    #[test]
    fn parse_modulo_stats_value_replace_iif_indexin_rebin_and_broadcast() {
        use casa_coordinates::{
            CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
            StokesCoordinate, StokesType,
        };
        use casa_types::measures::direction::DirectionRef;
        use casa_types::measures::frequency::FrequencyRef;

        // 1. Modulo % and masked sample statistics variance/stddev/avdev
        let mut img = make_test_image(vec![4], 0.0);
        img.put_slice(
            &ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[4]), vec![2.0, 4.0, 6.0, 100.0])
                .unwrap(),
            &[0],
        )
        .unwrap();
        img.make_mask("m", true, true).unwrap();
        img.put_mask(
            "m",
            &ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[4]), vec![true, true, true, false])
                .unwrap(),
        )
        .unwrap();
        img.set_default_mask("m").unwrap();

        let r = make_resolver(&[("a", &img)]);
        let mod_e = parse_image_expr("value('a') % 4.0", &r).unwrap();
        assert_eq!(
            mod_e.get().unwrap().as_slice().unwrap(),
            &[2.0, 0.0, 2.0, 0.0]
        );

        let var_e = parse_image_expr("variance('a')", &r).unwrap();
        let std_e = parse_image_expr("stddev('a')", &r).unwrap();
        let avd_e = parse_image_expr("avdev('a')", &r).unwrap();
        // Valid values [2, 4, 6]: mean=4, var=((4+0+4)/2)=4, std=2, avdev=(2+0+2)/3=4/3
        assert!((var_e.get_at(&[]).unwrap() - 4.0).abs() < 1e-5);
        assert!((std_e.get_at(&[]).unwrap() - 2.0).abs() < 1e-5);
        assert!((avd_e.get_at(&[]).unwrap() - 4.0 / 3.0).abs() < 1e-5);

        // MaskExprBuilder::source_mask propagation
        let mb = ImageExprBuilder::from_image(&img).unwrap().gt_scalar(1.0);
        assert_eq!(
            mb.source_mask().unwrap().unwrap().as_slice().unwrap(),
            &[true, true, true, false]
        );

        // 2. value() strips mask, replace() preserves primary mask
        let val_c = parse_image_expr("value('a')", &r).unwrap();
        assert!(val_c.source_mask().is_none());
        let rep_c = parse_image_expr("replace('a', 99.0)", &r).unwrap();
        assert_eq!(
            rep_c.get().unwrap().as_slice().unwrap(),
            &[2.0, 4.0, 6.0, 99.0]
        );
        assert_eq!(
            rep_c.source_mask().unwrap().as_slice().unwrap(),
            &[true, true, true, false]
        );

        // 3. iif() conditional mask propagation + indexin()
        let mut b = make_test_image(vec![4], 10.0);
        b.make_mask("mb", true, true).unwrap();
        b.put_mask(
            "mb",
            &ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[4]), vec![false, true, true, true])
                .unwrap(),
        )
        .unwrap();
        b.set_default_mask("mb").unwrap();
        let r2 = make_resolver(&[("a", &img), ("b", &b)]);
        let iif_c = parse_image_expr("iif(indexin(0, [0, 2:3]), 'a', 'b')", &r2).unwrap();
        // idx 0->a(valid), 1->b(valid), 2->a(valid), 3->a(invalid)
        assert_eq!(
            iif_c.get().unwrap().as_slice().unwrap(),
            &[2.0, 10.0, 6.0, 100.0]
        );
        assert_eq!(
            iif_c.source_mask().unwrap().as_slice().unwrap(),
            &[true, true, true, false]
        );

        // 4. 4D + 2D and 4D + 4D_deg broadcasting + rebin + spectralindex + pa
        let mut cs1 = CoordinateSystem::new();
        cs1.add_coordinate(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [0.0, 0.0],
            [-1e-4, 1e-4],
            [0.0, 0.0],
        ));
        cs1.add_coordinate(StokesCoordinate::new(vec![StokesType::Q, StokesType::U]));
        cs1.add_coordinate(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            0.0,
            1.0e9,
            1.0e6,
            1.0e9,
        ));
        let mut cs2 = cs1.clone();
        cs2.replace_coordinate(
            2,
            SpectralCoordinate::new(FrequencyRef::LSRK, 0.0, 2.0e9, 1.0e6, 2.0e9),
        );

        let mut i4d = TempImage::<f32>::new(
            vec![2, 2, 2, 2],
            cs1.clone(),
            casa_lattices::TempStoragePolicy::Memory,
        )
        .unwrap();
        i4d.set(4.0).unwrap();
        let i2d = make_test_image(vec![2, 2], 1.0);
        let r3 = make_resolver(&[("i4d", &i4d), ("i2d", &i2d)]);
        let bcast = parse_image_expr("'i4d' + 'i2d'", &r3).unwrap();
        assert_eq!(bcast.shape(), &[2, 2, 2, 2]);
        assert!((bcast.get_at(&[1, 1, 1, 1]).unwrap() - 5.0).abs() < 1e-5);

        assert!(parse_image_expr("rebin('i4d', [2, 2])", &r3).is_err());
        let rebinned = parse_image_expr("rebin('i4d', [2, 2, 1, 1])", &r3).unwrap();
        assert_eq!(rebinned.shape(), &[1, 1, 2, 2]);
        assert!((rebinned.get_at(&[0, 0, 1, 0]).unwrap() - 4.0).abs() < 1e-5);

        let mut sp_a = TempImage::<f32>::new(
            vec![2, 2, 1, 1],
            cs1,
            casa_lattices::TempStoragePolicy::Memory,
        )
        .unwrap();
        sp_a.set(1.0).unwrap();
        let mut sp_b = TempImage::<f32>::new(
            vec![2, 2, 1, 1],
            cs2,
            casa_lattices::TempStoragePolicy::Memory,
        )
        .unwrap();
        sp_b.set(2.0).unwrap();
        let r4 = make_resolver(&[("sa", &sp_a), ("sb", &sp_b)]);
        let spix = parse_image_expr("spectralindex('sa', 'sb')", &r4).unwrap();
        assert!((spix.get_at(&[0, 0, 0, 0]).unwrap() - 1.0).abs() < 1e-5);

        let pa = parse_image_expr("pa('sa', 'sb')", &r4).unwrap();
        let expected_pa = (0.5 * (1.0f64).atan2(2.0)).to_degrees() as f32;
        assert!((pa.get_at(&[0, 0, 0, 0]).unwrap() - expected_pa).abs() < 1e-4);
        assert_eq!(pa.units(), "deg");
    }
}
