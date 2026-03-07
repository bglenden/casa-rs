// SPDX-License-Identifier: LGPL-3.0-or-later
//! Parser for casacore-compatible Lattice Expression Language (LEL) strings.
//!
//! Converts a LEL expression string into the lazy [`ImageExpr`] / [`MaskExpr`]
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
//! only via the typed API on [`ImageExpr`], not through the parser (the parser
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
//! use casacore_coordinates::CoordinateSystem;
//! use casacore_images::{ImageExpr, TempImage};
//! use casacore_images::expr_parser::{parse_image_expr, HashMapResolver};
//! use casacore_lattices::LatticeMut;
//!
//! let mut a = TempImage::<f32>::new(vec![4, 4], CoordinateSystem::new()).unwrap();
//! a.set(2.0).unwrap();
//!
//! let mut images = HashMap::new();
//! images.insert("a".to_string(), &a as &dyn casacore_images::image::ImageInterface<f32>);
//! let resolver = HashMapResolver(images);
//!
//! let expr = parse_image_expr("sin('a') + 1.0", &resolver).unwrap();
//! let val = expr.get_at(&[0, 0]).unwrap();
//! assert!((val - (2.0_f32.sin() + 1.0)).abs() < 1e-6);
//! ```

use std::collections::HashMap;
use std::fmt;

use casacore_lattices::Lattice;
use casacore_types::ArrayD;
use ndarray::IxDyn;

use crate::error::ImageError;
use crate::image::ImageInterface;
use crate::image_expr::{
    ImageExpr, ImageExprBinaryOp, ImageExprCompareOp, ImageExprUnaryOp, ImageExprValue, MaskExpr,
};

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
        if let Some(c) = self.peek_char() {
            if c == 'e' || c == 'E' || c == 'd' || c == 'D' {
                self.advance_char();
                if let Some(s) = self.peek_char() {
                    if s == '+' || s == '-' {
                        self.advance_char();
                    }
                }
                while let Some(d) = self.peek_char() {
                    if d.is_ascii_digit() {
                        self.advance_char();
                    } else {
                        break;
                    }
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
        let text = self.src[start..self.pos].to_string();
        Ok((Token::Ident(text), start))
    }
}

// ---------------------------------------------------------------------------
// Parser intermediate representation
// ---------------------------------------------------------------------------

/// Intermediate parse result: either a numeric expression or a boolean mask.
enum ExprNode<'a, T: ImageExprValue + PartialOrd + ExprValueConvert> {
    Numeric(Box<ImageExpr<'a, T>>),
    Scalar(f64),
    Mask(MaskExpr<'a, T>),
}

impl<'a, T: ImageExprValue + PartialOrd + ExprValueConvert> ExprNode<'a, T> {
    fn into_numeric(self, pos: usize) -> Result<ImageExpr<'a, T>, ExprParseError> {
        match self {
            Self::Numeric(e) => Ok(*e),
            Self::Scalar(_) => Err(ExprParseError {
                message: "bare scalar cannot be used as an image expression (no shape context)"
                    .into(),
                position: pos,
            }),
            Self::Mask(_) => Err(ExprParseError {
                message: "expected numeric expression but found boolean mask".into(),
                position: pos,
            }),
        }
    }

    fn into_mask(self, pos: usize) -> Result<MaskExpr<'a, T>, ExprParseError> {
        match self {
            Self::Mask(m) => Ok(m),
            _ => Err(ExprParseError {
                message: "expected boolean mask expression but found numeric".into(),
                position: pos,
            }),
        }
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
        match (&lhs, &rhs) {
            (ExprNode::Scalar(a), ExprNode::Scalar(b)) => {
                // Both scalars: compute at parse time
                let a_t = Self::convert_f64_to_t(*a);
                let b_t = Self::convert_f64_to_t(*b);
                let result = apply_binary_op(a_t, b_t, op);
                Ok(ExprNode::Scalar(result.to_f64()))
            }
            (ExprNode::Scalar(s), ExprNode::Numeric(img)) => {
                // scalar op image
                match op {
                    // Commutative ops: rewrite as image op scalar
                    ImageExprBinaryOp::Add
                    | ImageExprBinaryOp::Multiply
                    | ImageExprBinaryOp::Min
                    | ImageExprBinaryOp::Max => Ok(ExprNode::Numeric(Box::new(
                        (**img)
                            .clone()
                            .binary_scalar(Self::convert_f64_to_t(*s), op),
                    ))),
                    // Non-commutative: use scalar_left_binary for correct Inf/NaN handling
                    _ => Ok(ExprNode::Numeric(Box::new(ImageExpr::scalar_left_binary(
                        Self::convert_f64_to_t(*s),
                        (**img).clone(),
                        op,
                    )))),
                }
            }
            (ExprNode::Numeric(img), ExprNode::Scalar(s)) => Ok(ExprNode::Numeric(Box::new(
                (**img)
                    .clone()
                    .binary_scalar(Self::convert_f64_to_t(*s), op),
            ))),
            (ExprNode::Numeric(_), ExprNode::Numeric(_)) => {
                let lhs_expr = lhs.into_numeric(pos)?;
                let rhs_expr = rhs.into_numeric(pos)?;
                Ok(ExprNode::Numeric(Box::new(
                    lhs_expr
                        .binary_expr(rhs_expr, op)
                        .map_err(|e| ExprParseError {
                            message: e.to_string(),
                            position: pos,
                        })?,
                )))
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

            // At least one side must be a numeric expression (not bare scalar
            // without shape context).
            match (&left, &right) {
                (ExprNode::Scalar(_), ExprNode::Scalar(_)) => {
                    return Err(ExprParseError {
                        message: "comparison between two scalars has no image shape context".into(),
                        position: pos,
                    });
                }
                (ExprNode::Numeric(img), ExprNode::Scalar(s)) => {
                    let mask = (**img)
                        .clone()
                        .compare_scalar(Self::convert_f64_to_t(*s), op);
                    return Ok(ExprNode::Mask(mask));
                }
                (ExprNode::Scalar(s), ExprNode::Numeric(img)) => {
                    // Flip the comparison: scalar op image => image flipped_op scalar
                    let flipped = flip_compare_op(op);
                    let mask = (**img)
                        .clone()
                        .compare_scalar(Self::convert_f64_to_t(*s), flipped);
                    return Ok(ExprNode::Mask(mask));
                }
                (ExprNode::Numeric(_), ExprNode::Numeric(_)) => {
                    return Err(ExprParseError {
                        message: "image-to-image comparisons not yet supported; \
                                  compare each image against a scalar"
                            .into(),
                        position: pos,
                    });
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
                    ExprNode::Scalar(v) => Ok(ExprNode::Scalar(-v)),
                    ExprNode::Numeric(expr) => Ok(ExprNode::Numeric(Box::new((*expr).negate()))),
                    ExprNode::Mask(_) => Err(ExprParseError {
                        message: "cannot negate a boolean mask expression".into(),
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
            let saved_pos = self.lexer.pos;
            let saved_current_pos = self.current_pos;
            self.advance()?;
            if self.current == Token::LParen {
                return self.parse_function_call(&name, pos);
            }
            // Not a function call — treat as image reference
            // (we already advanced past the ident, so the current token is
            // whatever follows it)
            // Restore? No — we consumed the ident, and current is now the
            // next token.  We need to resolve the ident as an image name.
            let _ = (saved_pos, saved_current_pos);
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
        let arg2 = self.parse_expression()?;

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
            "pi" => Ok(ExprNode::Scalar(std::f64::consts::PI)),
            "e" => Ok(ExprNode::Scalar(std::f64::consts::E)),
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
            "ceil" => Some(ImageExprUnaryOp::Ceil),
            "floor" => Some(ImageExprUnaryOp::Floor),
            "round" => Some(ImageExprUnaryOp::Round),
            "sign" => Some(ImageExprUnaryOp::Sign),
            "conj" => Some(ImageExprUnaryOp::Conj),
            _ => None,
        };

        if let Some(op) = unary_op {
            match arg {
                ExprNode::Scalar(v) => {
                    let t = Self::convert_f64_to_t(v);
                    let result = apply_unary_op(t, op);
                    Ok(ExprNode::Scalar(result.to_f64()))
                }
                ExprNode::Numeric(expr) => Ok(ExprNode::Numeric(Box::new((*expr).unary(op)))),
                ExprNode::Mask(_) => Err(ExprParseError {
                    message: format!("function '{name}' requires a numeric argument, got mask"),
                    position: pos,
                }),
            }
        } else {
            // Wave 14 functions
            match lower.as_str() {
                "isnan" => match arg {
                    ExprNode::Scalar(v) => {
                        // Constant-fold: isnan of a literal
                        let is_nan = v.is_nan();
                        // We need a shape context to produce a MaskExpr; without an image
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
                    ExprNode::Numeric(expr) => Ok(ExprNode::Mask((*expr).isnan())),
                    ExprNode::Mask(_) => Err(ExprParseError {
                        message: "isnan() requires a numeric argument, got mask".into(),
                        position: pos,
                    }),
                },
                "ndim" => {
                    let expr = arg.into_numeric(pos)?;
                    Ok(ExprNode::Scalar(expr.ndim_value() as f64))
                }
                "nelem" | "nelements" => {
                    let expr = arg.into_numeric(pos)?;
                    Ok(ExprNode::Scalar(expr.nelem_value() as f64))
                }
                // Wave 14b: 1-arg reductions
                "sum" => {
                    let expr = arg.into_numeric(pos)?;
                    Ok(ExprNode::Numeric(Box::new(expr.sum_reduce())))
                }
                "min1d" | "min" => {
                    let expr = arg.into_numeric(pos)?;
                    Ok(ExprNode::Numeric(Box::new(expr.min_reduce())))
                }
                "max1d" | "max" => {
                    let expr = arg.into_numeric(pos)?;
                    Ok(ExprNode::Numeric(Box::new(expr.max_reduce())))
                }
                "mean1d" | "mean" => {
                    let expr = arg.into_numeric(pos)?;
                    Ok(ExprNode::Numeric(Box::new(expr.mean_reduce())))
                }
                "median1d" | "median" => {
                    let expr = arg.into_numeric(pos)?;
                    Ok(ExprNode::Numeric(Box::new(expr.median_reduce())))
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
                    Ok(ExprNode::Numeric(Box::new(ImageExpr::ntrue(mask))))
                }
                "nfalse" => {
                    let mask = arg.into_mask(pos)?;
                    Ok(ExprNode::Numeric(Box::new(ImageExpr::nfalse(mask))))
                }
                "mask" => {
                    // Returns the default pixel mask of the argument image.
                    // Propagates source masks through the built-in numeric DAG
                    // when possible; otherwise falls back to all-true.
                    let expr = arg.into_numeric(pos)?;
                    let shape = expr.shape().to_vec();
                    let mask = match expr.source_mask() {
                        Ok(Some(m)) => m,
                        _ => ArrayD::from_elem(IxDyn(&shape), true),
                    };
                    Ok(ExprNode::Mask(MaskExpr::from_constant(mask)))
                }
                "value" => {
                    // Identity: return the expression unchanged (strips mask).
                    match arg {
                        ExprNode::Numeric(_) => Ok(arg),
                        ExprNode::Scalar(_) => Ok(arg),
                        ExprNode::Mask(_) => Err(ExprParseError {
                            message: "value() requires a numeric argument, got mask".into(),
                            position: pos,
                        }),
                    }
                }
                // Wave 14d: type-changing functions (parser error, typed API only)
                "real" | "imag" | "arg" | "complex" => Err(ExprParseError {
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
            return self.combine_binary(arg1, arg2, op, pos);
        }

        // Wave 14 2-arg functions
        match lower.as_str() {
            "length" => {
                let expr = arg1.into_numeric(pos)?;
                let axis = match arg2 {
                    ExprNode::Scalar(v) => v as usize,
                    _ => {
                        return Err(ExprParseError {
                            message: "length() second argument must be a scalar axis index".into(),
                            position: pos,
                        });
                    }
                };
                match expr.length_value(axis) {
                    Some(len) => Ok(ExprNode::Scalar(len as f64)),
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
                    ExprNode::Scalar(v) => v,
                    _ => {
                        return Err(ExprParseError {
                            message: "fractile1d() second argument must be a scalar fraction"
                                .into(),
                            position: pos,
                        });
                    }
                };
                Ok(ExprNode::Numeric(Box::new(expr.fractile(frac))))
            }
            "fractilerange1d" | "fractilerange" => {
                // 2-arg variant: fractilerange1d(expr, frac) = range from frac to 1-frac
                let expr = arg1.into_numeric(pos)?;
                let frac = match arg2 {
                    ExprNode::Scalar(v) => v,
                    _ => {
                        return Err(ExprParseError {
                            message: "fractilerange1d() second argument must be a scalar fraction"
                                .into(),
                            position: pos,
                        });
                    }
                };
                Ok(ExprNode::Numeric(Box::new(
                    expr.fractile_range(frac, 1.0 - frac),
                )))
            }
            "replace" => {
                // replace(image, replacement) — replaces masked-out pixels.
                // Reads the primary's propagated pixel mask when available.
                let primary = arg1.into_numeric(pos)?;
                let shape = primary.shape().to_vec();
                let replacement = match arg2 {
                    ExprNode::Scalar(s) => ImageExpr::scalar(T::from_f64(s)),
                    ExprNode::Numeric(r) => *r,
                    ExprNode::Mask(_) => {
                        return Err(ExprParseError {
                            message: "replace() second argument must be numeric".into(),
                            position: pos,
                        });
                    }
                };
                let mask = match primary.source_mask() {
                    Ok(Some(m)) => m,
                    _ => ArrayD::from_elem(IxDyn(&shape), true),
                };
                primary
                    .replace(replacement, mask)
                    .map_err(|e| ExprParseError {
                        message: e.to_string(),
                        position: pos,
                    })
                    .map(|r| ExprNode::Numeric(Box::new(r)))
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
                // iif(condition, true_val, false_val)
                let condition = arg1.into_mask(pos)?;
                let if_true = arg2.into_numeric(pos)?;
                let if_false = arg3.into_numeric(pos)?;
                ImageExpr::iif(condition, if_true, if_false)
                    .map_err(|e| ExprParseError {
                        message: e.to_string(),
                        position: pos,
                    })
                    .map(|r| ExprNode::Numeric(Box::new(r)))
            }
            "fractilerange1d" | "fractilerange" => {
                // fractilerange1d(expr, frac1, frac2)
                let expr = arg1.into_numeric(pos)?;
                let frac1 = match arg2 {
                    ExprNode::Scalar(v) => v,
                    _ => {
                        return Err(ExprParseError {
                            message: "fractilerange1d() second argument must be a scalar".into(),
                            position: pos,
                        });
                    }
                };
                let frac2 = match arg3 {
                    ExprNode::Scalar(v) => v,
                    _ => {
                        return Err(ExprParseError {
                            message: "fractilerange1d() third argument must be a scalar".into(),
                            position: pos,
                        });
                    }
                };
                Ok(ExprNode::Numeric(Box::new(
                    expr.fractile_range(frac1, frac2),
                )))
            }
            _ => Err(ExprParseError {
                message: format!("unknown 3-argument function: {name}"),
                position: pos,
            }),
        }
    }

    fn resolve_image(&self, name: &str, pos: usize) -> Result<ExprNode<'a, T>, ExprParseError> {
        let image = self.resolver.resolve(name).map_err(|e| ExprParseError {
            message: e.to_string(),
            position: pos,
        })?;
        let expr = ImageExpr::from_dyn(image).map_err(|e| ExprParseError {
            message: e.to_string(),
            position: pos,
        })?;
        Ok(ExprNode::Numeric(Box::new(expr)))
    }

    fn parse_primary(&mut self) -> Result<ExprNode<'a, T>, ExprParseError> {
        match self.current.clone() {
            Token::LParen => {
                self.advance()?;
                let inner = self.parse_expression()?;
                self.expect(&Token::RParen)?;
                Ok(inner)
            }
            Token::Number(value) => {
                self.advance()?;
                Ok(ExprNode::Scalar(value))
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

// ---------------------------------------------------------------------------
// Operator application helpers
// ---------------------------------------------------------------------------

fn apply_unary_op<T: ImageExprValue>(value: T, op: ImageExprUnaryOp) -> T {
    match op {
        ImageExprUnaryOp::Negate => -value,
        ImageExprUnaryOp::Exp => value.expr_exp(),
        ImageExprUnaryOp::Sin => value.expr_sin(),
        ImageExprUnaryOp::Cos => value.expr_cos(),
        ImageExprUnaryOp::Tan => value.expr_tan(),
        ImageExprUnaryOp::Asin => value.expr_asin(),
        ImageExprUnaryOp::Acos => value.expr_acos(),
        ImageExprUnaryOp::Atan => value.expr_atan(),
        ImageExprUnaryOp::Sinh => value.expr_sinh(),
        ImageExprUnaryOp::Cosh => value.expr_cosh(),
        ImageExprUnaryOp::Tanh => value.expr_tanh(),
        ImageExprUnaryOp::Log => value.expr_log(),
        ImageExprUnaryOp::Log10 => value.expr_log10(),
        ImageExprUnaryOp::Sqrt => value.expr_sqrt(),
        ImageExprUnaryOp::Abs => value.expr_abs(),
        ImageExprUnaryOp::Ceil => value.expr_ceil(),
        ImageExprUnaryOp::Floor => value.expr_floor(),
        ImageExprUnaryOp::Round => value.expr_round(),
        ImageExprUnaryOp::Sign => value.expr_sign(),
        ImageExprUnaryOp::Conj => value.expr_conj(),
    }
}

fn apply_binary_op<T: ImageExprValue>(lhs: T, rhs: T, op: ImageExprBinaryOp) -> T {
    match op {
        ImageExprBinaryOp::Add => lhs + rhs,
        ImageExprBinaryOp::Subtract => lhs - rhs,
        ImageExprBinaryOp::Multiply => lhs * rhs,
        ImageExprBinaryOp::Divide => lhs / rhs,
        ImageExprBinaryOp::Pow => lhs.expr_pow(rhs),
        ImageExprBinaryOp::Fmod => lhs.expr_fmod(rhs),
        ImageExprBinaryOp::Atan2 => lhs.expr_atan2(rhs),
        ImageExprBinaryOp::Min => lhs.expr_min(rhs),
        ImageExprBinaryOp::Max => lhs.expr_max(rhs),
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

/// Parses a LEL expression string and returns a numeric [`ImageExpr`].
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
/// # use casacore_images::expr_parser::*;
/// # use casacore_images::PagedImage;
/// # use std::collections::HashMap;
/// let a = PagedImage::<f32>::open("a.image").unwrap();
/// let mut map = HashMap::new();
/// map.insert("a.image".to_string(), &a as &dyn casacore_images::image::ImageInterface<f32>);
/// let resolver = HashMapResolver(map);
/// let expr = parse_image_expr("'a.image' * 2.0 + 1.0", &resolver).unwrap();
/// ```
pub fn parse_image_expr<'a, T, R>(
    expr: &str,
    resolver: &R,
) -> Result<ImageExpr<'a, T>, ExprParseError>
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

/// Parses a LEL expression string and returns a boolean [`MaskExpr`].
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
/// # use casacore_images::expr_parser::*;
/// # use casacore_images::PagedImage;
/// # use std::collections::HashMap;
/// let a = PagedImage::<f32>::open("a.image").unwrap();
/// let mut map = HashMap::new();
/// map.insert("a.image".to_string(), &a as &dyn casacore_images::image::ImageInterface<f32>);
/// let resolver = HashMapResolver(map);
/// let mask = parse_mask_expr("'a.image' > 0.5 && 'a.image' < 10.0", &resolver).unwrap();
/// ```
pub fn parse_mask_expr<'a, T, R>(
    expr: &str,
    resolver: &R,
) -> Result<MaskExpr<'a, T>, ExprParseError>
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
}

impl ExprValueConvert for f32 {
    fn from_f64(v: f64) -> Self {
        v as f32
    }
    fn to_f64(self) -> f64 {
        self as f64
    }
}

impl ExprValueConvert for f64 {
    fn from_f64(v: f64) -> Self {
        v
    }
    fn to_f64(self) -> f64 {
        self
    }
}

impl ExprValueConvert for casacore_types::Complex32 {
    fn from_f64(v: f64) -> Self {
        Self::new(v as f32, 0.0)
    }
    fn to_f64(self) -> f64 {
        self.re as f64
    }
}

impl ExprValueConvert for casacore_types::Complex64 {
    fn from_f64(v: f64) -> Self {
        Self::new(v, 0.0)
    }
    fn to_f64(self) -> f64 {
        self.re
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::temp_image::TempImage;
    use casacore_coordinates::CoordinateSystem;

    fn make_test_image(shape: Vec<usize>, value: f32) -> TempImage<f32> {
        let mut img = TempImage::<f32>::new(shape, CoordinateSystem::new()).unwrap();
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
    fn scalar_of(expr: &ImageExpr<f32>) -> f32 {
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
        let a = make_test_image(vec![2, 2], 1.0);
        let r = make_resolver(&[("a", &a)]);
        let err = parse_image_expr("real('a')", &r).unwrap_err();
        assert!(
            err.message.contains("typed API") || err.message.contains("type-changing"),
            "got: {}",
            err.message
        );
    }
}
