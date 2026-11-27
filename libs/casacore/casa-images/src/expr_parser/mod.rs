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
//! images.insert("a".to_string(), &a as &dyn casa_images::ImageInterface<f32>);
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

mod lexer;
mod syntax;

pub use lexer::referenced_images;
use lexer::*;
use syntax::*;

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

mod complex;
mod read_geometry;

use casa_core::Complex64;
use complex::{arithmetic_complex, compare_complex, complex_constructor, complex_function};
use read_geometry::Operator;

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
/// map.insert("a.image".to_string(), &a as &dyn casa_images::ImageInterface<f32>);
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
/// map.insert("a.image".to_string(), &a as &dyn casa_images::ImageInterface<f32>);
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

pub fn subimage_mask_builder<'a, T, R>(
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

pub fn parse_image_tool_mask<'a, R>(
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

pub fn image_tool_mask_builder<'a, R>(
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
    .map_err(|error| ImageError::CasaSelection(mask_error_wording(error)))?;
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

pub fn compile_image_tool_pixels<'a, R>(
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
    .map_err(|error| ImageError::CasaSelection(mask_error_wording(error)))?;
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

pub enum ReplacementPixels {
    Real(ndarray::ArrayD<f64>),
    Complex(ndarray::ArrayD<Complex64>),
}

pub fn evaluate_replacement<'a, R>(
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

pub fn evaluate_real_scalar<'a, R>(
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

impl ExprValueConvert for casa_core::Complex32 {
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

impl ExprValueConvert for casa_core::Complex64 {
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

/// The parser's own wording, as CASA reports a refused mask expression.
pub fn mask_error_wording(error: ExprParseError) -> String {
    if error.message.starts_with("unknown image") {
        // The resolver's miss: CASA's ImageExprParse wording.
        error
            .message
            .replace("unknown image: ", "ImageExprParse: '")
            + "' is an unknown lattice or image or it is an unqualified region"
    } else {
        format!("Image Expression: {error}")
    }
}

#[cfg(test)]
mod tests;
