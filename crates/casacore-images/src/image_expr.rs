// SPDX-License-Identifier: LGPL-3.0-or-later
// Arc is used for Clone on expression closures; they intentionally
// capture non-Send `&dyn ImageInterface` references.
#![allow(clippy::arc_with_non_send_sync)]
//! Lazy image expressions corresponding to C++ `ImageExpr<T>`.
//!
//! The expression tree borrows its source images and evaluates pixels only
//! for the requested slice or point, without materializing full intermediate
//! arrays.
//!
//! Supported operator families:
//!
//! - **Arithmetic**: add, subtract, multiply, divide (image/image and
//!   image/scalar)
//! - **Unary numeric transforms**: negation, abs, ceil, floor, round, sign,
//!   conj
//! - **Transcendental functions**: exp, sin, cos, tan, asin, acos, atan,
//!   sinh, cosh, tanh, log, log10, sqrt
//! - **Binary math functions**: pow, fmod, atan2, min, max
//! - **Comparison and logical composition**: `>`, `<`, `>=`, `<=`, `==`,
//!   `!=`, AND, OR, NOT via [`MaskExpr`]
//! - **NaN test**: `isnan(expr)` via [`MaskExpr`]
//! - **Metadata queries**: `ndim`, `nelem`, `length`
//! - **Reductions**: `sum`, `min1d`, `max1d`, `mean1d`, `median1d`,
//!   `fractile1d`, `fractilerange1d`
//! - **Conditional**: `iif(cond, true, false)`
//! - **Mask operations**: `all`, `any`, `ntrue`, `nfalse`, `mask`, `value`,
//!   `replace`
//! - **Type-changing** (typed API only): `real_part`, `imag_part`,
//!   `arg_phase`, `to_complex`
//!
//! # LEL Parity Status (C++ casacore LEL surface)
//!
//! | Category | Total | Parser | Typed API | Notes |
//! |----------|-------|--------|-----------|-------|
//! | Unary operators | 3 | 3 | — | `-`, `+`, `!` |
//! | Binary operators | 10 | 10 | — | `+ - * / ^ == != > >= < <=` + `&& \|\|` |
//! | 0-arg functions | 2 | 2 | — | `pi()`, `e()` |
//! | 1-arg math | 19 | 19 | — | sin … conj |
//! | 1-arg mask | 3 | 3 | — | isnan, all, any |
//! | 1-arg reduction | 7 | 7 | — | sum, min, max, mean, median, ntrue, nfalse (→ 0-D scalar) |
//! | 1-arg metadata | 4 | 4 | — | ndim, nelem, mask¹, value |
//! | 2-arg functions | 8 | 8 | — | pow, fmod, atan2, min, max, length, fractile, replace¹ |
//! | 3-arg functions | 2 | 2 | — | iif, fractilerange (3-arg) |
//! | Type-changing | 4 | — | 4 | real, imag, arg, complex² |
//! | Deferred | 1 | — | — | `indexin` (requires array literal syntax) |
//!
//! **Parser-accessible: 56/57.** Type-changing functions (4) are typed API only.
//! INDEXIN is deferred.
//!
//! ¹ `mask()` and `replace()` propagate source pixel masks through the
//!   built-in unary/binary numeric DAG so expressions like `mask(a + 1)` and
//!   `replace(a * 2, 0)` keep the source mask. Opaque typed closures and some
//!   non-elementwise nodes still fall back to all-true.
//!
//! ² The parser is monomorphic in `T`; type-changing functions require
//!   calling [`ImageExpr::real_part`], [`ImageExpr::imag_part`],
//!   [`ImageExpr::arg_phase`], or [`ImageExpr::to_complex`] directly.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

use casacore_coordinates::CoordinateSystem;
use casacore_lattices::execution::{
    ParallelReadChunkConfig, ReadChunkExecutionStrategy, try_fold_traversal_cursors,
    try_for_each_traversal_cursor, try_reduce_read_chunks,
};
use casacore_lattices::{
    Lattice, LatticeError, LatticeStatistics, Statistic, TraversalCursorIter, TraversalSpec,
};
use casacore_types::ArrayD;
use ndarray::{IxDyn, Zip};

use crate::error::ImageError;
use crate::image::{ImageInterface, ImagePixel, PagedImage};
use crate::image_info::ImageInfo;

#[path = "image_expr_compiled.rs"]
mod compiled;
pub use compiled::{CompiledImageExpr, CompiledMaskExpr};

type UnaryExprFn<T> = Arc<dyn Fn(T) -> T + Send + Sync>;
type BinaryExprFn<T> = Arc<dyn Fn(T, T) -> T + Send + Sync>;

#[derive(Clone, Copy)]
struct ExprReductionPartial<T> {
    seen: bool,
    value: T,
}

impl<T> ExprReductionPartial<T> {
    fn new(value: T) -> Self {
        Self { seen: false, value }
    }
}

/// Numeric operations supported by the lazy image expression DAG.
///
/// Extends [`ImagePixel`] with mathematical functions dispatched by
/// [`ImageExprUnaryOp`] and [`ImageExprBinaryOp`].  Implemented for
/// [`f32`], [`f64`], [`Complex32`](casacore_types::Complex32), and
/// [`Complex64`](casacore_types::Complex64).
pub trait ImageExprValue:
    ImagePixel
    + std::ops::Add<Output = Self>
    + std::ops::Sub<Output = Self>
    + std::ops::Mul<Output = Self>
    + std::ops::Div<Output = Self>
    + std::ops::Neg<Output = Self>
{
    // Transcendental / elementary functions
    fn expr_exp(self) -> Self;
    fn expr_sin(self) -> Self;
    fn expr_cos(self) -> Self;
    fn expr_tan(self) -> Self;
    fn expr_asin(self) -> Self;
    fn expr_acos(self) -> Self;
    fn expr_atan(self) -> Self;
    fn expr_sinh(self) -> Self;
    fn expr_cosh(self) -> Self;
    fn expr_tanh(self) -> Self;
    fn expr_log(self) -> Self;
    fn expr_log10(self) -> Self;
    fn expr_sqrt(self) -> Self;
    fn expr_abs(self) -> Self;
    fn expr_conj(self) -> Self;

    // Rounding / sign (component-wise for complex)
    fn expr_ceil(self) -> Self;
    fn expr_floor(self) -> Self;
    fn expr_round(self) -> Self;
    fn expr_sign(self) -> Self;

    // Binary math functions
    fn expr_pow(self, rhs: Self) -> Self;
    fn expr_fmod(self, rhs: Self) -> Self;
    fn expr_atan2(self, rhs: Self) -> Self;
    fn expr_min(self, rhs: Self) -> Self;
    fn expr_max(self, rhs: Self) -> Self;

    /// Returns `true` if this value is NaN.
    ///
    /// For complex types, returns `true` if either component is NaN.
    fn expr_isnan(&self) -> bool;

    /// Lossily convert from `f64`.  Used by reduction/scalar operations.
    fn from_f64_lossy(v: f64) -> Self;

    /// Lossily convert to `f64`.  Used by sorting in median/fractile.
    fn to_f64_lossy(&self) -> f64;

    /// Optional fast path for direct source-backed global reductions.
    fn reduction_from_source(
        _op: ReductionOp,
        _image: &dyn ImageInterface<Self>,
    ) -> Option<Result<Self, LatticeError>>
    where
        Self: Sized,
    {
        None
    }

    /// Compare two values using LEL-style scalar comparison semantics.
    fn expr_compare(self, rhs: Self, op: ImageExprCompareOp) -> bool;
}

impl ImageExprValue for f32 {
    fn expr_exp(self) -> Self {
        self.exp()
    }
    fn expr_sin(self) -> Self {
        self.sin()
    }
    fn expr_cos(self) -> Self {
        self.cos()
    }
    fn expr_tan(self) -> Self {
        self.tan()
    }
    fn expr_asin(self) -> Self {
        self.asin()
    }
    fn expr_acos(self) -> Self {
        self.acos()
    }
    fn expr_atan(self) -> Self {
        self.atan()
    }
    fn expr_sinh(self) -> Self {
        self.sinh()
    }
    fn expr_cosh(self) -> Self {
        self.cosh()
    }
    fn expr_tanh(self) -> Self {
        self.tanh()
    }
    fn expr_log(self) -> Self {
        self.ln()
    }
    fn expr_log10(self) -> Self {
        self.log10()
    }
    fn expr_sqrt(self) -> Self {
        self.sqrt()
    }
    fn expr_abs(self) -> Self {
        self.abs()
    }
    fn expr_conj(self) -> Self {
        self
    }
    fn expr_ceil(self) -> Self {
        self.ceil()
    }
    fn expr_floor(self) -> Self {
        self.floor()
    }
    fn expr_round(self) -> Self {
        self.round()
    }
    fn expr_sign(self) -> Self {
        if self > 0.0 {
            1.0
        } else if self < 0.0 {
            -1.0
        } else {
            0.0
        }
    }
    fn expr_pow(self, rhs: Self) -> Self {
        self.powf(rhs)
    }
    fn expr_fmod(self, rhs: Self) -> Self {
        self % rhs
    }
    fn expr_atan2(self, rhs: Self) -> Self {
        self.atan2(rhs)
    }
    fn expr_min(self, rhs: Self) -> Self {
        f32::min(self, rhs)
    }
    fn expr_max(self, rhs: Self) -> Self {
        f32::max(self, rhs)
    }
    fn expr_isnan(&self) -> bool {
        self.is_nan()
    }
    fn from_f64_lossy(v: f64) -> Self {
        v as f32
    }
    fn to_f64_lossy(&self) -> f64 {
        *self as f64
    }

    fn reduction_from_source(
        op: ReductionOp,
        image: &dyn ImageInterface<Self>,
    ) -> Option<Result<Self, LatticeError>> {
        Some(source_numeric_reduction_value(op, image))
    }

    fn expr_compare(self, rhs: Self, op: ImageExprCompareOp) -> bool {
        match op {
            ImageExprCompareOp::GreaterThan => self > rhs,
            ImageExprCompareOp::LessThan => self < rhs,
            ImageExprCompareOp::GreaterEqual => self >= rhs,
            ImageExprCompareOp::LessEqual => self <= rhs,
            ImageExprCompareOp::Equal => self == rhs,
            ImageExprCompareOp::NotEqual => self != rhs,
        }
    }
}

impl ImageExprValue for f64 {
    fn expr_exp(self) -> Self {
        self.exp()
    }
    fn expr_sin(self) -> Self {
        self.sin()
    }
    fn expr_cos(self) -> Self {
        self.cos()
    }
    fn expr_tan(self) -> Self {
        self.tan()
    }
    fn expr_asin(self) -> Self {
        self.asin()
    }
    fn expr_acos(self) -> Self {
        self.acos()
    }
    fn expr_atan(self) -> Self {
        self.atan()
    }
    fn expr_sinh(self) -> Self {
        self.sinh()
    }
    fn expr_cosh(self) -> Self {
        self.cosh()
    }
    fn expr_tanh(self) -> Self {
        self.tanh()
    }
    fn expr_log(self) -> Self {
        self.ln()
    }
    fn expr_log10(self) -> Self {
        self.log10()
    }
    fn expr_sqrt(self) -> Self {
        self.sqrt()
    }
    fn expr_abs(self) -> Self {
        self.abs()
    }
    fn expr_conj(self) -> Self {
        self
    }
    fn expr_ceil(self) -> Self {
        self.ceil()
    }
    fn expr_floor(self) -> Self {
        self.floor()
    }
    fn expr_round(self) -> Self {
        self.round()
    }
    fn expr_sign(self) -> Self {
        if self > 0.0 {
            1.0
        } else if self < 0.0 {
            -1.0
        } else {
            0.0
        }
    }
    fn expr_pow(self, rhs: Self) -> Self {
        self.powf(rhs)
    }
    fn expr_fmod(self, rhs: Self) -> Self {
        self % rhs
    }
    fn expr_atan2(self, rhs: Self) -> Self {
        self.atan2(rhs)
    }
    fn expr_min(self, rhs: Self) -> Self {
        f64::min(self, rhs)
    }
    fn expr_max(self, rhs: Self) -> Self {
        f64::max(self, rhs)
    }
    fn expr_isnan(&self) -> bool {
        self.is_nan()
    }
    fn from_f64_lossy(v: f64) -> Self {
        v
    }
    fn to_f64_lossy(&self) -> f64 {
        *self
    }

    fn reduction_from_source(
        op: ReductionOp,
        image: &dyn ImageInterface<Self>,
    ) -> Option<Result<Self, LatticeError>> {
        Some(source_numeric_reduction_value(op, image))
    }

    fn expr_compare(self, rhs: Self, op: ImageExprCompareOp) -> bool {
        match op {
            ImageExprCompareOp::GreaterThan => self > rhs,
            ImageExprCompareOp::LessThan => self < rhs,
            ImageExprCompareOp::GreaterEqual => self >= rhs,
            ImageExprCompareOp::LessEqual => self <= rhs,
            ImageExprCompareOp::Equal => self == rhs,
            ImageExprCompareOp::NotEqual => self != rhs,
        }
    }
}

impl ImageExprValue for casacore_types::Complex32 {
    fn expr_exp(self) -> Self {
        self.exp()
    }
    fn expr_sin(self) -> Self {
        self.sin()
    }
    fn expr_cos(self) -> Self {
        self.cos()
    }
    fn expr_tan(self) -> Self {
        self.tan()
    }
    fn expr_asin(self) -> Self {
        self.asin()
    }
    fn expr_acos(self) -> Self {
        self.acos()
    }
    fn expr_atan(self) -> Self {
        self.atan()
    }
    fn expr_sinh(self) -> Self {
        self.sinh()
    }
    fn expr_cosh(self) -> Self {
        self.cosh()
    }
    fn expr_tanh(self) -> Self {
        self.tanh()
    }
    fn expr_log(self) -> Self {
        self.ln()
    }
    fn expr_log10(self) -> Self {
        self.ln() / Self::new(std::f32::consts::LN_10, 0.0)
    }
    fn expr_sqrt(self) -> Self {
        self.sqrt()
    }
    fn expr_abs(self) -> Self {
        Self::new(self.norm(), 0.0)
    }
    fn expr_conj(self) -> Self {
        self.conj()
    }
    fn expr_ceil(self) -> Self {
        Self::new(self.re.ceil(), self.im.ceil())
    }
    fn expr_floor(self) -> Self {
        Self::new(self.re.floor(), self.im.floor())
    }
    fn expr_round(self) -> Self {
        Self::new(self.re.round(), self.im.round())
    }
    fn expr_sign(self) -> Self {
        let n = self.norm();
        if n == 0.0 {
            Self::new(0.0, 0.0)
        } else {
            self / Self::new(n, 0.0)
        }
    }
    fn expr_pow(self, rhs: Self) -> Self {
        self.powc(rhs)
    }
    fn expr_fmod(self, rhs: Self) -> Self {
        Self::new(self.re % rhs.re, self.im % rhs.im)
    }
    fn expr_atan2(self, rhs: Self) -> Self {
        Self::new(self.re.atan2(rhs.re), self.im.atan2(rhs.im))
    }
    fn expr_min(self, rhs: Self) -> Self {
        if self.norm_sqr() <= rhs.norm_sqr() {
            self
        } else {
            rhs
        }
    }
    fn expr_max(self, rhs: Self) -> Self {
        if self.norm_sqr() >= rhs.norm_sqr() {
            self
        } else {
            rhs
        }
    }
    fn expr_isnan(&self) -> bool {
        self.re.is_nan() || self.im.is_nan()
    }
    fn from_f64_lossy(v: f64) -> Self {
        Self::new(v as f32, 0.0)
    }
    fn to_f64_lossy(&self) -> f64 {
        self.norm() as f64
    }

    fn expr_compare(self, rhs: Self, op: ImageExprCompareOp) -> bool {
        match op {
            ImageExprCompareOp::Equal => self == rhs,
            ImageExprCompareOp::NotEqual => self != rhs,
            ImageExprCompareOp::GreaterThan => self.norm_sqr() > rhs.norm_sqr(),
            ImageExprCompareOp::LessThan => self.norm_sqr() < rhs.norm_sqr(),
            ImageExprCompareOp::GreaterEqual => self.norm_sqr() >= rhs.norm_sqr(),
            ImageExprCompareOp::LessEqual => self.norm_sqr() <= rhs.norm_sqr(),
        }
    }
}

impl ImageExprValue for casacore_types::Complex64 {
    fn expr_exp(self) -> Self {
        self.exp()
    }
    fn expr_sin(self) -> Self {
        self.sin()
    }
    fn expr_cos(self) -> Self {
        self.cos()
    }
    fn expr_tan(self) -> Self {
        self.tan()
    }
    fn expr_asin(self) -> Self {
        self.asin()
    }
    fn expr_acos(self) -> Self {
        self.acos()
    }
    fn expr_atan(self) -> Self {
        self.atan()
    }
    fn expr_sinh(self) -> Self {
        self.sinh()
    }
    fn expr_cosh(self) -> Self {
        self.cosh()
    }
    fn expr_tanh(self) -> Self {
        self.tanh()
    }
    fn expr_log(self) -> Self {
        self.ln()
    }
    fn expr_log10(self) -> Self {
        self.ln() / Self::new(std::f64::consts::LN_10, 0.0)
    }
    fn expr_sqrt(self) -> Self {
        self.sqrt()
    }
    fn expr_abs(self) -> Self {
        Self::new(self.norm(), 0.0)
    }
    fn expr_conj(self) -> Self {
        self.conj()
    }
    fn expr_ceil(self) -> Self {
        Self::new(self.re.ceil(), self.im.ceil())
    }
    fn expr_floor(self) -> Self {
        Self::new(self.re.floor(), self.im.floor())
    }
    fn expr_round(self) -> Self {
        Self::new(self.re.round(), self.im.round())
    }
    fn expr_sign(self) -> Self {
        let n = self.norm();
        if n == 0.0 {
            Self::new(0.0, 0.0)
        } else {
            self / Self::new(n, 0.0)
        }
    }
    fn expr_pow(self, rhs: Self) -> Self {
        self.powc(rhs)
    }
    fn expr_fmod(self, rhs: Self) -> Self {
        Self::new(self.re % rhs.re, self.im % rhs.im)
    }
    fn expr_atan2(self, rhs: Self) -> Self {
        Self::new(self.re.atan2(rhs.re), self.im.atan2(rhs.im))
    }
    fn expr_min(self, rhs: Self) -> Self {
        if self.norm_sqr() <= rhs.norm_sqr() {
            self
        } else {
            rhs
        }
    }
    fn expr_max(self, rhs: Self) -> Self {
        if self.norm_sqr() >= rhs.norm_sqr() {
            self
        } else {
            rhs
        }
    }
    fn expr_isnan(&self) -> bool {
        self.re.is_nan() || self.im.is_nan()
    }
    fn from_f64_lossy(v: f64) -> Self {
        Self::new(v, 0.0)
    }
    fn to_f64_lossy(&self) -> f64 {
        self.norm()
    }

    fn expr_compare(self, rhs: Self, op: ImageExprCompareOp) -> bool {
        match op {
            ImageExprCompareOp::Equal => self == rhs,
            ImageExprCompareOp::NotEqual => self != rhs,
            ImageExprCompareOp::GreaterThan => self.norm_sqr() > rhs.norm_sqr(),
            ImageExprCompareOp::LessThan => self.norm_sqr() < rhs.norm_sqr(),
            ImageExprCompareOp::GreaterEqual => self.norm_sqr() >= rhs.norm_sqr(),
            ImageExprCompareOp::LessEqual => self.norm_sqr() <= rhs.norm_sqr(),
        }
    }
}

/// Unary operators on lazy numeric image expressions.
///
/// Corresponds to the C++ `LELUnaryEnums` plus the element-wise functions
/// from `LELFunctionEnums` that map one lattice to one lattice of the same
/// pixel type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ImageExprUnaryOp {
    /// Arithmetic negation: `-x`.
    Negate,
    /// Exponential: `exp(x)`.
    Exp,
    /// Sine: `sin(x)`.
    Sin,
    /// Cosine: `cos(x)`.
    Cos,
    /// Tangent: `tan(x)`.
    Tan,
    /// Arc sine: `asin(x)`.
    Asin,
    /// Arc cosine: `acos(x)`.
    Acos,
    /// Arc tangent: `atan(x)`.
    Atan,
    /// Hyperbolic sine: `sinh(x)`.
    Sinh,
    /// Hyperbolic cosine: `cosh(x)`.
    Cosh,
    /// Hyperbolic tangent: `tanh(x)`.
    Tanh,
    /// Natural logarithm: `log(x)`.
    Log,
    /// Base-10 logarithm: `log10(x)`.
    Log10,
    /// Square root: `sqrt(x)`.
    Sqrt,
    /// Absolute value / modulus: `abs(x)`.
    ///
    /// For complex types returns `Complex(|z|, 0)` to preserve the pixel type.
    Abs,
    /// Ceiling: `ceil(x)`.  Component-wise for complex.
    Ceil,
    /// Floor: `floor(x)`.  Component-wise for complex.
    Floor,
    /// Round to nearest integer: `round(x)`.  Component-wise for complex.
    Round,
    /// Sign: `signum(x)`.  For complex: `z / |z|` (or zero).
    Sign,
    /// Complex conjugate: `conj(x)`.  Identity for real types.
    Conj,
}

/// Binary operators on lazy numeric image expressions.
///
/// Includes both arithmetic operators and element-wise math functions
/// corresponding to C++ `LELBinaryEnums` and two-argument entries in
/// `LELFunctionEnums`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ImageExprBinaryOp {
    /// Addition: `lhs + rhs`.
    Add,
    /// Subtraction: `lhs - rhs`.
    Subtract,
    /// Multiplication: `lhs * rhs`.
    Multiply,
    /// Division: `lhs / rhs`.
    Divide,
    /// Power: `pow(lhs, rhs)`.
    Pow,
    /// Floating-point remainder: `fmod(lhs, rhs)`.  Component-wise for complex.
    Fmod,
    /// Two-argument arc tangent: `atan2(lhs, rhs)`.  Component-wise for complex.
    Atan2,
    /// Element-wise minimum: `min(lhs, rhs)`.  By norm for complex.
    Min,
    /// Element-wise maximum: `max(lhs, rhs)`.  By norm for complex.
    Max,
}

/// Comparison operators for lazy mask expressions derived from images.
///
/// Corresponds to the relational subset of C++ `LELBinaryEnums`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ImageExprCompareOp {
    /// Greater-than comparison: `lhs > rhs`.
    GreaterThan,
    /// Less-than comparison: `lhs < rhs`.
    LessThan,
    /// Greater-or-equal comparison: `lhs >= rhs`.
    GreaterEqual,
    /// Less-or-equal comparison: `lhs <= rhs`.
    LessEqual,
    /// Equality comparison: `lhs == rhs`.
    Equal,
    /// Inequality comparison: `lhs != rhs`.
    NotEqual,
}

/// Logical operators for lazy boolean mask expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MaskLogicalOp {
    /// Boolean AND: `lhs && rhs`.
    And,
    /// Boolean OR: `lhs || rhs`.
    Or,
}

/// Reduction operations for collapsing an entire image to a scalar.
///
/// Corresponds to the C++ `LELFunctionEnums` reduction family:
/// `SUM`, `MIN1D`, `MAX1D`, `MEAN1D`, `MEDIAN1D`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReductionOp {
    /// Sum of all elements.
    Sum,
    /// Minimum element.
    Min,
    /// Maximum element.
    Max,
    /// Mean (arithmetic average) of all elements.
    Mean,
    /// Median of all elements (real types only).
    Median,
}

#[derive(Clone)]
struct ImageExprMeta {
    shape: Vec<usize>,
    coords: CoordinateSystem,
    units: String,
    misc_info: casacore_types::RecordValue,
    image_info: ImageInfo,
    name: Option<PathBuf>,
}

impl ImageExprMeta {
    fn from_image<T: ImageExprValue>(image: &dyn ImageInterface<T>) -> Result<Self, ImageError> {
        Ok(Self {
            shape: image.shape().to_vec(),
            coords: image.coordinates().clone(),
            units: image.units().to_string(),
            misc_info: image.misc_info(),
            image_info: image.image_info()?,
            name: image.name().map(Path::to_path_buf),
        })
    }
}

/// Boxed slice-evaluation closure for type-erased mask/bridge operations.
///
/// Used by `Conditional`, `MaskCount`, and `TypeBridge` to avoid propagating
/// `PartialOrd` or cross-type bounds into `NumericExprNode`.  We use `Arc`
/// (not `Rc`) for `Clone` on the enum, but these closures are intentionally
/// not `Send + Sync` because `ImageInterface` references are not thread-safe.
type SliceEvalFn<'a, T> =
    Arc<dyn Fn(&[usize], &[usize], &[usize]) -> Result<ArrayD<T>, LatticeError> + 'a>;

#[derive(Clone)]
#[allow(clippy::arc_with_non_send_sync)]
enum NumericExprNode<'a, T: ImageExprValue> {
    Source(&'a dyn ImageInterface<T>),
    Scalar(T),
    UnaryOp {
        op: ImageExprUnaryOp,
        child: Box<NumericExprNode<'a, T>>,
    },
    BinaryOp {
        op: ImageExprBinaryOp,
        lhs: Box<NumericExprNode<'a, T>>,
        rhs: Box<NumericExprNode<'a, T>>,
    },
    CustomUnary {
        name: &'static str,
        child: Box<NumericExprNode<'a, T>>,
        func: UnaryExprFn<T>,
    },
    CustomBinary {
        name: &'static str,
        lhs: Box<NumericExprNode<'a, T>>,
        rhs: Box<NumericExprNode<'a, T>>,
        func: BinaryExprFn<T>,
    },
    /// Reduction (sum, min, max, mean, median) of the full child array to a scalar.
    Reduction {
        op: ReductionOp,
        child: Box<NumericExprNode<'a, T>>,
        child_shape: Vec<usize>,
    },
    /// Fractile (quantile) of the full child array — real types only.
    Fractile {
        child: Box<NumericExprNode<'a, T>>,
        child_shape: Vec<usize>,
        fraction: f64,
    },
    /// Difference between two fractiles — real types only.
    FractileRange {
        child: Box<NumericExprNode<'a, T>>,
        child_shape: Vec<usize>,
        fraction1: f64,
        fraction2: f64,
    },
    /// Conditional: `iif(cond, true_val, false_val)`.
    Conditional {
        condition: Box<MaskExprNode<'a, T>>,
        if_true: Box<NumericExprNode<'a, T>>,
        if_false: Box<NumericExprNode<'a, T>>,
    },
    /// Count of true or false values in a boolean mask, cast to T.
    MaskCount {
        count_true: bool,
        mask: Box<MaskExprNode<'a, T>>,
        mask_shape: Vec<usize>,
    },
    /// Replace pixels where mask is false with a replacement value.
    Replace {
        primary: Box<NumericExprNode<'a, T>>,
        replacement: Box<NumericExprNode<'a, T>>,
        mask: ArrayD<bool>,
    },
    /// Type bridge: an opaque evaluation closure for cross-type operations.
    TypeBridge {
        eval_fn: SliceEvalFn<'a, T>,
    },
}

impl<'a, T: ImageExprValue> NumericExprNode<'a, T> {
    fn preferred_cursor_shape(&self, full_shape: &[usize]) -> Vec<usize> {
        match self {
            Self::Source(image) => clamp_cursor_shape(&image.nice_cursor_shape(), full_shape),
            Self::UnaryOp { child, .. }
            | Self::CustomUnary { child, .. }
            | Self::Reduction { child, .. }
            | Self::Fractile { child, .. }
            | Self::FractileRange { child, .. } => child.preferred_cursor_shape(full_shape),
            Self::BinaryOp { lhs, .. } | Self::CustomBinary { lhs, .. } => {
                lhs.preferred_cursor_shape(full_shape)
            }
            Self::Conditional { if_true, .. } => if_true.preferred_cursor_shape(full_shape),
            Self::Replace { primary, .. } => primary.preferred_cursor_shape(full_shape),
            Self::Scalar(_) | Self::MaskCount { .. } | Self::TypeBridge { .. } => {
                advised_cursor_shape(full_shape)
            }
        }
    }

    fn try_shape(&self) -> Option<Vec<usize>> {
        match self {
            Self::Source(image) => Some(image.shape().to_vec()),
            Self::Scalar(_) => Some(vec![]),
            Self::UnaryOp { child, .. } | Self::CustomUnary { child, .. } => child.try_shape(),
            Self::BinaryOp { lhs, rhs, .. } | Self::CustomBinary { lhs, rhs, .. } => {
                let lhs_shape = lhs.try_shape()?;
                let rhs_shape = rhs.try_shape()?;
                broadcast_shapes(&lhs_shape, &rhs_shape).ok()
            }
            Self::Reduction { .. }
            | Self::Fractile { .. }
            | Self::FractileRange { .. }
            | Self::MaskCount { .. } => Some(vec![]),
            Self::Conditional {
                condition,
                if_true,
                if_false,
            } => {
                let condition_shape = condition.try_shape()?;
                let if_true_shape = if_true.try_shape()?;
                let if_false_shape = if_false.try_shape()?;
                let value_shape = broadcast_shapes(&if_true_shape, &if_false_shape).ok()?;
                broadcast_shapes(&condition_shape, &value_shape).ok()
            }
            Self::Replace { primary, .. } => primary.try_shape(),
            Self::TypeBridge { .. } => None,
        }
    }

    fn propagated_mask(&self) -> Result<Option<ArrayD<bool>>, ImageError> {
        match self {
            Self::Source(image) => image.default_mask(),
            Self::Scalar(_) => Ok(None),
            Self::UnaryOp { child, .. } | Self::CustomUnary { child, .. } => {
                child.propagated_mask()
            }
            Self::BinaryOp { lhs, rhs, .. } | Self::CustomBinary { lhs, rhs, .. } => {
                let lhs_shape = lhs.try_shape();
                let rhs_shape = rhs.try_shape();
                match (lhs_shape, rhs_shape) {
                    (Some(lhs_shape), Some(rhs_shape)) => combine_optional_masks(
                        lhs.propagated_mask()?,
                        &lhs_shape,
                        rhs.propagated_mask()?,
                        &rhs_shape,
                    ),
                    _ => Ok(None),
                }
            }
            Self::Conditional { .. }
            | Self::Reduction { .. }
            | Self::Fractile { .. }
            | Self::FractileRange { .. }
            | Self::MaskCount { .. }
            | Self::Replace { .. }
            | Self::TypeBridge { .. } => Ok(None),
        }
    }

    fn eval_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError> {
        match self {
            Self::Source(image) => image.get_slice(start, shape, stride),
            Self::Scalar(value) => Ok(ArrayD::from_elem(IxDyn(shape), *value)),
            Self::UnaryOp { op, child } => {
                let mut out = child.eval_slice(start, shape, stride)?;
                out.mapv_inplace(|value| match op {
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
                });
                Ok(out)
            }
            Self::BinaryOp { op, lhs, rhs } => {
                let lhs_data = lhs.eval_slice(start, shape, stride)?;
                let rhs_data = rhs.eval_slice(start, shape, stride)?;
                Ok(Zip::from(&lhs_data)
                    .and(&rhs_data)
                    .map_collect(|&left, &right| match op {
                        ImageExprBinaryOp::Add => left + right,
                        ImageExprBinaryOp::Subtract => left - right,
                        ImageExprBinaryOp::Multiply => left * right,
                        ImageExprBinaryOp::Divide => left / right,
                        ImageExprBinaryOp::Pow => left.expr_pow(right),
                        ImageExprBinaryOp::Fmod => left.expr_fmod(right),
                        ImageExprBinaryOp::Atan2 => left.expr_atan2(right),
                        ImageExprBinaryOp::Min => left.expr_min(right),
                        ImageExprBinaryOp::Max => left.expr_max(right),
                    }))
            }
            Self::CustomUnary {
                name: _name,
                child,
                func,
            } => {
                let mut out = child.eval_slice(start, shape, stride)?;
                let func = Arc::clone(func);
                out.mapv_inplace(|value| (func)(value));
                Ok(out)
            }
            Self::CustomBinary {
                name: _name,
                lhs,
                rhs,
                func,
            } => {
                let lhs_data = lhs.eval_slice(start, shape, stride)?;
                let rhs_data = rhs.eval_slice(start, shape, stride)?;
                let func = Arc::clone(func);
                Ok(Zip::from(&lhs_data)
                    .and(&rhs_data)
                    .map_collect(|&lhs, &rhs| (func)(lhs, rhs)))
            }
            Self::Reduction {
                op,
                child,
                child_shape,
            } => {
                let result = match op {
                    ReductionOp::Sum => reduce_numeric_expr(
                        child,
                        Some(ReductionOp::Sum),
                        child_shape,
                        std::mem::size_of::<T>(),
                        1_048_576,
                        T::default_value,
                        |acc, chunk| {
                            for &v in chunk.iter() {
                                *acc = *acc + v;
                            }
                            Ok(())
                        },
                        |acc, other| {
                            *acc = *acc + other;
                            Ok(())
                        },
                    )?,
                    ReductionOp::Min => {
                        let partial = reduce_numeric_expr(
                            child,
                            Some(ReductionOp::Min),
                            child_shape,
                            std::mem::size_of::<ExprReductionPartial<T>>(),
                            1_048_576,
                            || ExprReductionPartial::new(T::default_value()),
                            |partial, chunk| {
                                for &v in chunk.iter() {
                                    if !partial.seen {
                                        partial.value = v;
                                        partial.seen = true;
                                    } else {
                                        partial.value = partial.value.expr_min(v);
                                    }
                                }
                                Ok(())
                            },
                            |partial, other| {
                                if other.seen {
                                    if !partial.seen {
                                        *partial = other;
                                    } else {
                                        partial.value = partial.value.expr_min(other.value);
                                    }
                                }
                                Ok(())
                            },
                        )?;
                        if partial.seen {
                            partial.value
                        } else {
                            T::default_value()
                        }
                    }
                    ReductionOp::Max => {
                        let partial = reduce_numeric_expr(
                            child,
                            Some(ReductionOp::Max),
                            child_shape,
                            std::mem::size_of::<ExprReductionPartial<T>>(),
                            1_048_576,
                            || ExprReductionPartial::new(T::default_value()),
                            |partial, chunk| {
                                for &v in chunk.iter() {
                                    if !partial.seen {
                                        partial.value = v;
                                        partial.seen = true;
                                    } else {
                                        partial.value = partial.value.expr_max(v);
                                    }
                                }
                                Ok(())
                            },
                            |partial, other| {
                                if other.seen {
                                    if !partial.seen {
                                        *partial = other;
                                    } else {
                                        partial.value = partial.value.expr_max(other.value);
                                    }
                                }
                                Ok(())
                            },
                        )?;
                        if partial.seen {
                            partial.value
                        } else {
                            T::default_value()
                        }
                    }
                    ReductionOp::Mean => {
                        let (mut acc, n) = reduce_numeric_expr(
                            child,
                            Some(ReductionOp::Mean),
                            child_shape,
                            std::mem::size_of::<T>() + std::mem::size_of::<usize>(),
                            1_048_576,
                            || (T::default_value(), 0usize),
                            |partial, chunk| {
                                partial.1 += chunk.len();
                                for &v in chunk.iter() {
                                    partial.0 = partial.0 + v;
                                }
                                Ok(())
                            },
                            |partial, other| {
                                partial.0 = partial.0 + other.0;
                                partial.1 += other.1;
                                Ok(())
                            },
                        )?;
                        if n > 0 {
                            acc = acc * T::from_f64_lossy(1.0 / n as f64);
                        }
                        acc
                    }
                    ReductionOp::Median => {
                        let reserve = child_shape.iter().product::<usize>();
                        let mut vals = reduce_numeric_expr(
                            child,
                            Some(ReductionOp::Median),
                            child_shape,
                            reserve.saturating_mul(std::mem::size_of::<T>()),
                            4 * 1024 * 1024,
                            || Vec::with_capacity(reserve / thread_parallelism().max(1)),
                            |vals, chunk| {
                                vals.extend(chunk.iter().copied());
                                Ok(())
                            },
                            |vals, other| {
                                vals.extend(other);
                                Ok(())
                            },
                        )?;
                        vals.sort_by(|a, b| {
                            a.to_f64_lossy()
                                .partial_cmp(&b.to_f64_lossy())
                                .unwrap_or(std::cmp::Ordering::Equal)
                        });
                        let n = vals.len();
                        if n == 0 {
                            T::default_value()
                        } else if n % 2 == 1 {
                            vals[n / 2]
                        } else {
                            let a = vals[n / 2 - 1];
                            let b = vals[n / 2];
                            (a + b) * T::from_f64_lossy(0.5)
                        }
                    }
                };
                Ok(ArrayD::from_elem(IxDyn(shape), result))
            }
            Self::Fractile {
                child,
                child_shape,
                fraction,
            } => {
                let reserve = child_shape.iter().product::<usize>();
                let mut vals = reduce_numeric_expr(
                    child,
                    None,
                    child_shape,
                    reserve.saturating_mul(std::mem::size_of::<T>()),
                    4 * 1024 * 1024,
                    || Vec::with_capacity(reserve / thread_parallelism().max(1)),
                    |vals, chunk| {
                        vals.extend(chunk.iter().copied());
                        Ok(())
                    },
                    |vals, other| {
                        vals.extend(other);
                        Ok(())
                    },
                )?;
                vals.sort_by(|a, b| {
                    a.to_f64_lossy()
                        .partial_cmp(&b.to_f64_lossy())
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                let n = vals.len();
                let idx = (fraction * (n.saturating_sub(1)) as f64).floor() as usize;
                let result = vals
                    .get(idx.min(n.saturating_sub(1)))
                    .cloned()
                    .unwrap_or_else(T::default_value);
                Ok(ArrayD::from_elem(IxDyn(shape), result))
            }
            Self::FractileRange {
                child,
                child_shape,
                fraction1,
                fraction2,
            } => {
                let reserve = child_shape.iter().product::<usize>();
                let mut vals = reduce_numeric_expr(
                    child,
                    None,
                    child_shape,
                    reserve.saturating_mul(std::mem::size_of::<T>()),
                    4 * 1024 * 1024,
                    || Vec::with_capacity(reserve / thread_parallelism().max(1)),
                    |vals, chunk| {
                        vals.extend(chunk.iter().copied());
                        Ok(())
                    },
                    |vals, other| {
                        vals.extend(other);
                        Ok(())
                    },
                )?;
                vals.sort_by(|a, b| {
                    a.to_f64_lossy()
                        .partial_cmp(&b.to_f64_lossy())
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                let n = vals.len();
                let idx1 = (fraction1 * (n.saturating_sub(1)) as f64).floor() as usize;
                let idx2 = (fraction2 * (n.saturating_sub(1)) as f64).floor() as usize;
                let v1 = vals
                    .get(idx1.min(n.saturating_sub(1)))
                    .cloned()
                    .unwrap_or_else(T::default_value);
                let v2 = vals
                    .get(idx2.min(n.saturating_sub(1)))
                    .cloned()
                    .unwrap_or_else(T::default_value);
                let result = v2 - v1;
                Ok(ArrayD::from_elem(IxDyn(shape), result))
            }
            Self::Conditional {
                condition,
                if_true,
                if_false,
            } => {
                let cond = condition.eval_slice(start, shape, stride)?;
                let t_data = if_true.eval_slice(start, shape, stride)?;
                let f_data = if_false.eval_slice(start, shape, stride)?;
                Ok(Zip::from(&cond).and(&t_data).and(&f_data).map_collect(
                    |&cond, &if_true, &if_false| {
                        if cond { if_true } else { if_false }
                    },
                ))
            }
            Self::MaskCount {
                count_true,
                mask,
                mask_shape,
            } => {
                let mut count = 0usize;
                let cursor_shape = advised_cursor_shape(mask_shape);
                let cursors = TraversalCursorIter::new(
                    mask_shape.to_vec(),
                    cursor_shape.clone(),
                    TraversalSpec::chunks(cursor_shape),
                );
                for cursor in cursors {
                    let cursor = cursor?;
                    let stride = vec![1; cursor.position.len()];
                    let chunk = mask.eval_slice(&cursor.position, &cursor.shape, &stride)?;
                    count += if *count_true {
                        chunk.iter().filter(|&&v| v).count()
                    } else {
                        chunk.iter().filter(|&&v| !v).count()
                    };
                }
                Ok(ArrayD::from_elem(
                    IxDyn(shape),
                    T::from_f64_lossy(count as f64),
                ))
            }
            Self::Replace {
                primary,
                replacement,
                mask,
            } => {
                let p_data = primary.eval_slice(start, shape, stride)?;
                let r_data = replacement.eval_slice(start, shape, stride)?;
                // Slice the mask to the requested region
                let ndim = mask.ndim();
                let slice_info: Vec<ndarray::SliceInfoElem> = (0..ndim)
                    .map(|ax| {
                        let end = start[ax] + shape[ax] * stride[ax];
                        ndarray::SliceInfoElem::Slice {
                            start: start[ax] as isize,
                            end: Some(end as isize),
                            step: stride[ax] as isize,
                        }
                    })
                    .collect();
                let mask_slice = mask.slice(slice_info.as_slice());
                Ok(Zip::from(&mask_slice)
                    .and(&p_data)
                    .and(&r_data)
                    .map_collect(
                        |&mask, &primary, &replacement| {
                            if mask { primary } else { replacement }
                        },
                    ))
            }
            Self::TypeBridge { eval_fn } => eval_fn(start, shape, stride),
        }
    }

    fn kind_name(&self) -> &'static str {
        match self {
            Self::Source(_) => "source",
            Self::Scalar(_) => "scalar",
            Self::UnaryOp { .. } => "unary-op",
            Self::BinaryOp { .. } => "binary-op",
            Self::CustomUnary { name, .. } => name,
            Self::CustomBinary { name, .. } => name,
            Self::Reduction { .. } => "reduction",
            Self::Fractile { .. } => "fractile",
            Self::FractileRange { .. } => "fractile-range",
            Self::Conditional { .. } => "conditional",
            Self::MaskCount { .. } => "mask-count",
            Self::Replace { .. } => "replace",
            Self::TypeBridge { .. } => "type-bridge",
        }
    }
}

#[derive(Clone)]
enum MaskExprNode<'a, T: ImageExprValue> {
    CompareScalar {
        op: ImageExprCompareOp,
        expr: Box<NumericExprNode<'a, T>>,
        scalar: T,
    },
    Logical {
        op: MaskLogicalOp,
        lhs: Box<MaskExprNode<'a, T>>,
        rhs: Box<MaskExprNode<'a, T>>,
    },
    Not {
        child: Box<MaskExprNode<'a, T>>,
    },
    /// Element-wise NaN test: `isnan(expr)`.
    IsNan {
        child: Box<NumericExprNode<'a, T>>,
    },
    /// A constant boolean mask (e.g. from `mask(image)`).
    ConstantMask {
        mask: ArrayD<bool>,
    },
    /// Reduce a boolean mask to a single all-true/all-false value, broadcast.
    AllReduce {
        child: Box<MaskExprNode<'a, T>>,
        child_shape: Vec<usize>,
    },
    /// Reduce a boolean mask to a single any-true value, broadcast.
    AnyReduce {
        child: Box<MaskExprNode<'a, T>>,
        child_shape: Vec<usize>,
    },
}

impl<'a, T: ImageExprValue> MaskExprNode<'a, T> {
    fn preferred_cursor_shape(&self, full_shape: &[usize]) -> Vec<usize> {
        match self {
            Self::CompareScalar { expr, .. } => expr.preferred_cursor_shape(full_shape),
            Self::Logical { lhs, .. } => lhs.preferred_cursor_shape(full_shape),
            Self::Not { child } | Self::AllReduce { child, .. } | Self::AnyReduce { child, .. } => {
                child.preferred_cursor_shape(full_shape)
            }
            Self::IsNan { child } => child.preferred_cursor_shape(full_shape),
            Self::ConstantMask { .. } => advised_cursor_shape(full_shape),
        }
    }

    fn try_shape(&self) -> Option<Vec<usize>> {
        match self {
            Self::CompareScalar { expr, .. } | Self::IsNan { child: expr } => expr.try_shape(),
            Self::Logical { lhs, rhs, .. } => {
                let lhs_shape = lhs.try_shape()?;
                let rhs_shape = rhs.try_shape()?;
                broadcast_shapes(&lhs_shape, &rhs_shape).ok()
            }
            Self::Not { child } => child.try_shape(),
            Self::ConstantMask { mask } => Some(mask.shape().to_vec()),
            Self::AllReduce { .. } | Self::AnyReduce { .. } => Some(vec![]),
        }
    }

    fn eval_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<bool>, LatticeError> {
        match self {
            Self::CompareScalar { op, expr, scalar } => {
                let data = expr.eval_slice(start, shape, stride)?;
                Ok(data.mapv(|value| value.expr_compare(*scalar, *op)))
            }
            Self::Logical { op, lhs, rhs } => {
                let lhs_mask = lhs.eval_slice(start, shape, stride)?;
                let rhs_mask = rhs.eval_slice(start, shape, stride)?;
                Ok(Zip::from(&lhs_mask)
                    .and(&rhs_mask)
                    .map_collect(|&lhs, &rhs| match op {
                        MaskLogicalOp::And => lhs && rhs,
                        MaskLogicalOp::Or => lhs || rhs,
                    }))
            }
            Self::Not { child } => {
                let mut out = child.eval_slice(start, shape, stride)?;
                out.mapv_inplace(|value| !value);
                Ok(out)
            }
            Self::IsNan { child } => {
                let data = child.eval_slice(start, shape, stride)?;
                Ok(data.mapv(|v| v.expr_isnan()))
            }
            Self::ConstantMask { mask } => {
                // Slice the constant mask to the requested region.
                let ndim = mask.ndim();
                let slice_info: Vec<ndarray::SliceInfoElem> = (0..ndim)
                    .map(|ax| {
                        let end = start[ax] + shape[ax] * stride[ax];
                        ndarray::SliceInfoElem::Slice {
                            start: start[ax] as isize,
                            end: Some(end as isize),
                            step: stride[ax] as isize,
                        }
                    })
                    .collect();
                Ok(mask.slice(slice_info.as_slice()).to_owned())
            }
            Self::AllReduce { child, child_shape } => {
                let mut result = true;
                for_each_mask_chunk(child, child_shape, |chunk| {
                    if !chunk.iter().all(|&v| v) {
                        result = false;
                    }
                    Ok(())
                })?;
                Ok(ArrayD::from_elem(IxDyn(shape), result))
            }
            Self::AnyReduce { child, child_shape } => {
                let mut result = false;
                for_each_mask_chunk(child, child_shape, |chunk| {
                    if chunk.iter().any(|&v| v) {
                        result = true;
                    }
                    Ok(())
                })?;
                Ok(ArrayD::from_elem(IxDyn(shape), result))
            }
        }
    }
}

fn validate_slice_request(
    lattice_shape: &[usize],
    start: &[usize],
    shape: &[usize],
    stride: &[usize],
) -> Result<(), LatticeError> {
    let ndim = lattice_shape.len();
    if start.len() != ndim || shape.len() != ndim || stride.len() != ndim {
        return Err(LatticeError::NdimMismatch {
            expected: ndim,
            got: start.len().max(shape.len()).max(stride.len()),
        });
    }
    for axis in 0..ndim {
        if stride[axis] == 0 {
            return Err(LatticeError::SliceOutOfBounds {
                start: start.to_vec(),
                slice_shape: shape.to_vec(),
                stride: stride.to_vec(),
                lattice_shape: lattice_shape.to_vec(),
            });
        }
        if shape[axis] == 0 {
            continue;
        }
        if start[axis] >= lattice_shape[axis] {
            return Err(LatticeError::IndexOutOfBounds {
                index: start.to_vec(),
                shape: lattice_shape.to_vec(),
            });
        }
        let last = start[axis]
            .checked_add((shape[axis] - 1).saturating_mul(stride[axis]))
            .ok_or_else(|| LatticeError::SliceOutOfBounds {
                start: start.to_vec(),
                slice_shape: shape.to_vec(),
                stride: stride.to_vec(),
                lattice_shape: lattice_shape.to_vec(),
            })?;
        if last >= lattice_shape[axis] {
            return Err(LatticeError::IndexOutOfBounds {
                index: vec![last],
                shape: lattice_shape.to_vec(),
            });
        }
    }
    Ok(())
}

fn validate_same_shape(lhs: &[usize], rhs: &[usize]) -> Result<(), ImageError> {
    if lhs == rhs {
        Ok(())
    } else {
        Err(ImageError::ShapeMismatch {
            expected: lhs.to_vec(),
            got: rhs.to_vec(),
        })
    }
}

/// Returns the output shape when combining two operands.
///
/// C++ LEL allows scalar (0-D) operands to broadcast to the shape of the other
/// operand.  Both non-empty shapes must match exactly.
fn broadcast_shapes(lhs: &[usize], rhs: &[usize]) -> Result<Vec<usize>, ImageError> {
    if lhs.is_empty() || lhs == rhs {
        Ok(rhs.to_vec())
    } else if rhs.is_empty() {
        Ok(lhs.to_vec())
    } else {
        Err(ImageError::ShapeMismatch {
            expected: lhs.to_vec(),
            got: rhs.to_vec(),
        })
    }
}

fn broadcast_mask(
    mask: ArrayD<bool>,
    source_shape: &[usize],
    out_shape: &[usize],
) -> Result<ArrayD<bool>, ImageError> {
    if source_shape == out_shape {
        return Ok(mask);
    }
    let broadcast_shape = broadcast_shapes(source_shape, out_shape)?;
    if broadcast_shape != out_shape {
        return Err(ImageError::ShapeMismatch {
            expected: out_shape.to_vec(),
            got: source_shape.to_vec(),
        });
    }
    if source_shape.is_empty() {
        return Ok(ArrayD::from_elem(IxDyn(out_shape), mask[IxDyn(&[])]));
    }
    mask.broadcast(IxDyn(out_shape))
        .map(|view| view.to_owned())
        .ok_or_else(|| ImageError::ShapeMismatch {
            expected: out_shape.to_vec(),
            got: source_shape.to_vec(),
        })
}

fn combine_optional_masks(
    lhs_mask: Option<ArrayD<bool>>,
    lhs_shape: &[usize],
    rhs_mask: Option<ArrayD<bool>>,
    rhs_shape: &[usize],
) -> Result<Option<ArrayD<bool>>, ImageError> {
    let out_shape = broadcast_shapes(lhs_shape, rhs_shape)?;
    match (lhs_mask, rhs_mask) {
        (None, None) => Ok(None),
        (Some(mask), None) => broadcast_mask(mask, lhs_shape, &out_shape).map(Some),
        (None, Some(mask)) => broadcast_mask(mask, rhs_shape, &out_shape).map(Some),
        (Some(lhs), Some(rhs)) => {
            let lhs = broadcast_mask(lhs, lhs_shape, &out_shape)?;
            let rhs = broadcast_mask(rhs, rhs_shape, &out_shape)?;
            Ok(Some(
                Zip::from(&lhs)
                    .and(&rhs)
                    .map_collect(|&lhs, &rhs| lhs && rhs),
            ))
        }
    }
}

fn advised_cursor_shape(shape: &[usize]) -> Vec<usize> {
    let max_pixels = 1_048_576usize;
    if shape.is_empty() {
        return vec![];
    }
    let mut cursor = vec![1usize; shape.len()];
    let mut product = 1usize;
    for (axis, &extent) in shape.iter().enumerate() {
        let can_fit = max_pixels / product;
        if can_fit == 0 {
            break;
        }
        cursor[axis] = extent.min(can_fit);
        product *= cursor[axis];
    }
    cursor
}

fn clamp_cursor_shape(cursor_shape: &[usize], full_shape: &[usize]) -> Vec<usize> {
    if cursor_shape.len() != full_shape.len() {
        return advised_cursor_shape(full_shape);
    }
    cursor_shape
        .iter()
        .zip(full_shape.iter())
        .map(|(&cursor, &extent)| cursor.clamp(1, extent.max(1)))
        .collect()
}

fn thread_parallelism() -> usize {
    thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(1)
}

fn reduction_execution_strategy(
    full_shape: &[usize],
    cursor_shape: &[usize],
    per_worker_state_bytes: usize,
    large_work_threshold: usize,
) -> ReadChunkExecutionStrategy {
    let available = thread_parallelism();
    if available < 2 {
        return ReadChunkExecutionStrategy::Serial;
    }

    let task_count = TraversalCursorIter::new(
        full_shape.to_vec(),
        cursor_shape.to_vec(),
        TraversalSpec::chunks(cursor_shape.to_vec()),
    )
    .size_hint()
    .1
    .unwrap_or(0);
    if task_count < 4 || full_shape.iter().product::<usize>() < large_work_threshold {
        return ReadChunkExecutionStrategy::Serial;
    }

    if per_worker_state_bytes > 256 * 1024 * 1024 {
        return ReadChunkExecutionStrategy::Serial;
    }

    let workers = available.min(task_count.max(1));
    ReadChunkExecutionStrategy::Parallel(ParallelReadChunkConfig {
        workers,
        prefetch_depth: workers * 2,
    })
}

fn source_stats_reduction_value<T>(
    op: ReductionOp,
    image: &dyn ImageInterface<T>,
) -> Result<T, LatticeError>
where
    T: ImageExprValue + casacore_lattices::statistics::StatsElement,
{
    let stats = LatticeStatistics::new(image as &dyn Lattice<T>);
    let stat = match op {
        ReductionOp::Sum => Statistic::Sum,
        ReductionOp::Min => Statistic::Min,
        ReductionOp::Max => Statistic::Max,
        ReductionOp::Mean => Statistic::Mean,
        ReductionOp::Median => Statistic::Median,
    };
    let value = stats
        .get_statistic(stat)?
        .iter()
        .copied()
        .next()
        .unwrap_or_default();
    Ok(T::from_f64_lossy(value))
}

fn source_numeric_reduction_value<T>(
    op: ReductionOp,
    image: &dyn ImageInterface<T>,
) -> Result<T, LatticeError>
where
    T: ImageExprValue + casacore_lattices::statistics::StatsElement + Send + Sync,
{
    if image.shape().is_empty() {
        return image.get_at(&[]);
    }

    let full_shape = image.shape();
    let payload_bytes = full_shape
        .iter()
        .product::<usize>()
        .saturating_mul(std::mem::size_of::<T>());
    // TODO: Replace this small-image fallback with a dedicated serial scalar
    // source-reduction kernel. A simple direct full-read kernel regressed the
    // 64^3 Rust/C++ comparison, so the remaining optimization needs a tighter
    // loop that does not give up the medium-size win from the current path.
    if !matches!(op, ReductionOp::Median) && payload_bytes <= 4 * 1024 * 1024 {
        return source_stats_reduction_value(op, image);
    }

    let cursor_shape = clamp_cursor_shape(&image.nice_cursor_shape(), full_shape);
    let spec = TraversalSpec::chunks(cursor_shape.clone());

    match op {
        ReductionOp::Sum => try_reduce_read_chunks(
            image,
            spec,
            reduction_execution_strategy(
                full_shape,
                &cursor_shape,
                std::mem::size_of::<T>(),
                1_048_576,
            ),
            T::default_value,
            |acc, chunk| {
                for &v in &chunk.data {
                    *acc = *acc + v;
                }
                Ok(())
            },
            |acc, other| {
                *acc = *acc + other;
                Ok(())
            },
        ),
        ReductionOp::Mean => {
            let (mut sum, count) = try_reduce_read_chunks(
                image,
                spec,
                reduction_execution_strategy(
                    full_shape,
                    &cursor_shape,
                    std::mem::size_of::<T>() + std::mem::size_of::<usize>(),
                    1_048_576,
                ),
                || (T::default_value(), 0usize),
                |partial, chunk| {
                    partial.1 += chunk.data.len();
                    for &v in &chunk.data {
                        partial.0 = partial.0 + v;
                    }
                    Ok(())
                },
                |partial, other| {
                    partial.0 = partial.0 + other.0;
                    partial.1 += other.1;
                    Ok(())
                },
            )?;
            if count > 0 {
                sum = sum * T::from_f64_lossy(1.0 / count as f64);
            }
            Ok(sum)
        }
        ReductionOp::Min => {
            let partial = try_reduce_read_chunks(
                image,
                spec,
                reduction_execution_strategy(
                    full_shape,
                    &cursor_shape,
                    std::mem::size_of::<ExprReductionPartial<T>>(),
                    1_048_576,
                ),
                || ExprReductionPartial::new(T::default_value()),
                |partial, chunk| {
                    for &v in &chunk.data {
                        if !partial.seen {
                            partial.value = v;
                            partial.seen = true;
                        } else {
                            partial.value = partial.value.expr_min(v);
                        }
                    }
                    Ok(())
                },
                |partial, other| {
                    if other.seen {
                        if !partial.seen {
                            *partial = other;
                        } else {
                            partial.value = partial.value.expr_min(other.value);
                        }
                    }
                    Ok(())
                },
            )?;
            Ok(if partial.seen {
                partial.value
            } else {
                T::default_value()
            })
        }
        ReductionOp::Max => {
            let partial = try_reduce_read_chunks(
                image,
                spec,
                reduction_execution_strategy(
                    full_shape,
                    &cursor_shape,
                    std::mem::size_of::<ExprReductionPartial<T>>(),
                    1_048_576,
                ),
                || ExprReductionPartial::new(T::default_value()),
                |partial, chunk| {
                    for &v in &chunk.data {
                        if !partial.seen {
                            partial.value = v;
                            partial.seen = true;
                        } else {
                            partial.value = partial.value.expr_max(v);
                        }
                    }
                    Ok(())
                },
                |partial, other| {
                    if other.seen {
                        if !partial.seen {
                            *partial = other;
                        } else {
                            partial.value = partial.value.expr_max(other.value);
                        }
                    }
                    Ok(())
                },
            )?;
            Ok(if partial.seen {
                partial.value
            } else {
                T::default_value()
            })
        }
        ReductionOp::Median => source_stats_reduction_value(op, image),
    }
}

fn for_each_numeric_chunk<'a, T: ImageExprValue>(
    node: &NumericExprNode<'a, T>,
    full_shape: &[usize],
    mut f: impl FnMut(&ArrayD<T>) -> Result<(), LatticeError>,
) -> Result<(), LatticeError> {
    if full_shape.is_empty() {
        let empty = node.eval_slice(&[], &[], &[])?;
        return f(&empty);
    }
    let cursor_shape = node.preferred_cursor_shape(full_shape);
    try_for_each_traversal_cursor(
        full_shape,
        &cursor_shape,
        TraversalSpec::chunks(cursor_shape.clone()),
        |cursor| {
            let stride = vec![1; cursor.position.len()];
            let chunk = node.eval_slice(&cursor.position, &cursor.shape, &stride)?;
            f(&chunk)?;
            Ok(())
        },
    )
}

fn reduce_source_lattice<T, Part, Init, Process, Merge>(
    image: &dyn ImageInterface<T>,
    full_shape: &[usize],
    per_worker_state_bytes: usize,
    large_work_threshold: usize,
    make_partial: Init,
    process_chunk: Process,
    merge_partials: Merge,
) -> Result<Part, LatticeError>
where
    T: ImageExprValue,
    Part: Send,
    Init: Fn() -> Part + Sync + Send,
    Process: Fn(&mut Part, &ArrayD<T>) -> Result<(), LatticeError> + Sync + Send,
    Merge: Fn(&mut Part, Part) -> Result<(), LatticeError> + Sync,
{
    if full_shape.is_empty() {
        let chunk = image.get_slice(&[], &[], &[])?;
        let mut partial = make_partial();
        process_chunk(&mut partial, &chunk)?;
        return Ok(partial);
    }

    let payload_bytes = full_shape
        .iter()
        .product::<usize>()
        .saturating_mul(std::mem::size_of::<T>());
    if payload_bytes <= 16 * 1024 * 1024 {
        let chunk = image.get()?;
        let mut partial = make_partial();
        process_chunk(&mut partial, &chunk)?;
        return Ok(partial);
    }

    let cursor_shape = clamp_cursor_shape(&image.nice_cursor_shape(), full_shape);
    try_reduce_read_chunks(
        image,
        TraversalSpec::chunks(cursor_shape.clone()),
        reduction_execution_strategy(
            full_shape,
            &cursor_shape,
            per_worker_state_bytes,
            large_work_threshold,
        ),
        make_partial,
        |partial, chunk| process_chunk(partial, &chunk.data),
        merge_partials,
    )
}

fn reduce_numeric_expr<'a, T, Part, Init, Process, Merge>(
    node: &NumericExprNode<'a, T>,
    source_stat_op: Option<ReductionOp>,
    full_shape: &[usize],
    per_worker_state_bytes: usize,
    large_work_threshold: usize,
    make_partial: Init,
    process_chunk: Process,
    merge_partials: Merge,
) -> Result<Part, LatticeError>
where
    T: ImageExprValue,
    Part: Send,
    Init: Fn() -> Part + Sync + Send,
    Process: Fn(&mut Part, &ArrayD<T>) -> Result<(), LatticeError> + Sync + Send,
    Merge: Fn(&mut Part, Part) -> Result<(), LatticeError> + Sync,
{
    if let NumericExprNode::Source(image) = node {
        if let Some(op) = source_stat_op
            && let Some(result) = T::reduction_from_source(op, *image)
        {
            let value = result?;
            let mut partial = make_partial();
            let chunk = ArrayD::from_elem(IxDyn(&[]), value);
            process_chunk(&mut partial, &chunk)?;
            return Ok(partial);
        }
        return reduce_source_lattice(
            *image,
            full_shape,
            per_worker_state_bytes,
            large_work_threshold,
            make_partial,
            process_chunk,
            merge_partials,
        );
    }

    let mut partial = make_partial();
    for_each_numeric_chunk(node, full_shape, |chunk| process_chunk(&mut partial, chunk))?;
    Ok(partial)
}

fn for_each_mask_chunk<'a, T: ImageExprValue>(
    node: &MaskExprNode<'a, T>,
    full_shape: &[usize],
    mut f: impl FnMut(&ArrayD<bool>) -> Result<(), LatticeError>,
) -> Result<(), LatticeError> {
    if full_shape.is_empty() {
        let empty = node.eval_slice(&[], &[], &[])?;
        return f(&empty);
    }
    let cursor_shape = node.preferred_cursor_shape(full_shape);
    let cursors = TraversalCursorIter::new(
        full_shape.to_vec(),
        cursor_shape.clone(),
        TraversalSpec::chunks(cursor_shape),
    );
    for cursor in cursors {
        let cursor = cursor?;
        let stride = vec![1; cursor.position.len()];
        let chunk = node.eval_slice(&cursor.position, &cursor.shape, &stride)?;
        f(&chunk)?;
    }
    Ok(())
}

fn write_chunk_into_array<T: Clone>(
    out: &mut ArrayD<T>,
    start: &[usize],
    chunk: &ArrayD<T>,
) -> Result<(), LatticeError> {
    let ndim = out.ndim();
    if start.len() != ndim || chunk.ndim() != ndim {
        return Err(LatticeError::NdimMismatch {
            expected: ndim,
            got: start.len().max(chunk.ndim()),
        });
    }
    let slice_info: Vec<ndarray::SliceInfoElem> = start
        .iter()
        .zip(chunk.shape().iter())
        .map(|(&start, &extent)| ndarray::SliceInfoElem::Slice {
            start: start as isize,
            end: Some((start + extent) as isize),
            step: 1,
        })
        .collect();
    out.slice_mut(slice_info.as_slice()).assign(chunk);
    Ok(())
}

/// Read-only lazy image expression over typed image-like sources.
///
/// This is the Rust analogue of C++ `casacore::ImageExpr<T>`. It borrows its
/// source images and evaluates requested slices on demand rather than
/// precomputing a full result array.
pub struct ImageExpr<'a, T: ImageExprValue> {
    node: NumericExprNode<'a, T>,
    meta: ImageExprMeta,
    expr_string: Option<String>,
}

impl<'a, T: ImageExprValue> Clone for ImageExpr<'a, T> {
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            meta: self.meta.clone(),
            expr_string: self.expr_string.clone(),
        }
    }
}

impl<'a, T: ImageExprValue> fmt::Debug for ImageExpr<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImageExpr")
            .field("shape", &self.meta.shape)
            .field("pixel_type", &T::PRIMITIVE_TYPE)
            .field("root", &self.node.kind_name())
            .finish()
    }
}

impl<'a, T: ImageExprValue> ImageExpr<'a, T> {
    /// Creates a lazy expression rooted at a source image.
    pub fn from_image<I>(image: &'a I) -> Result<Self, ImageError>
    where
        I: ImageInterface<T> + 'a,
    {
        Ok(Self {
            node: NumericExprNode::Source(image),
            meta: ImageExprMeta::from_image(image)?,
            expr_string: None,
        })
    }

    pub(crate) fn scalar(value: T) -> Self {
        Self {
            node: NumericExprNode::Scalar(value),
            meta: ImageExprMeta {
                shape: vec![],
                coords: CoordinateSystem::new(),
                units: String::new(),
                misc_info: casacore_types::RecordValue::default(),
                image_info: ImageInfo::default(),
                name: None,
            },
            expr_string: None,
        }
    }

    /// Creates a lazy expression rooted at a trait-object image reference.
    ///
    /// This is the dynamic-dispatch counterpart of [`from_image`](Self::from_image),
    /// accepting `&dyn ImageInterface<T>` directly.  Used by the expression
    /// parser where image references are resolved at runtime.
    pub fn from_dyn(image: &'a dyn ImageInterface<T>) -> Result<Self, ImageError> {
        Ok(Self {
            node: NumericExprNode::Source(image),
            meta: ImageExprMeta::from_image(image)?,
            expr_string: None,
        })
    }

    fn custom_unary(
        self,
        name: &'static str,
        func: impl Fn(T) -> T + Send + Sync + 'static,
    ) -> Self {
        Self {
            node: NumericExprNode::CustomUnary {
                name,
                child: Box::new(self.node),
                func: Arc::new(func),
            },
            meta: self.meta,
            expr_string: None,
        }
    }

    fn custom_binary(self, rhs: Self, name: &'static str, func: BinaryExprFn<T>) -> Self {
        Self {
            node: NumericExprNode::CustomBinary {
                name,
                lhs: Box::new(self.node),
                rhs: Box::new(rhs.node),
                func,
            },
            meta: self.meta,
            expr_string: None,
        }
    }

    /// Compatibility constructor for the previous `map`-based API.
    ///
    /// The mapped function is now applied lazily to each requested slice.
    pub fn map<I, F>(image: &'a I, f: F) -> Result<Self, ImageError>
    where
        I: ImageInterface<T> + 'a,
        F: Fn(T) -> T + Send + Sync + 'static,
    {
        Ok(Self::from_image(image)?.custom_unary("map", f))
    }

    /// Compatibility constructor for the previous `zip`-based API.
    ///
    /// The combining function is now applied lazily to each requested slice.
    pub fn zip<I, J, F>(lhs: &'a I, rhs: &'a J, f: F) -> Result<Self, ImageError>
    where
        I: ImageInterface<T> + 'a,
        J: ImageInterface<T> + 'a,
        F: Fn(T, T) -> T + Send + Sync + 'static,
    {
        let lhs_expr = Self::from_image(lhs)?;
        let rhs_expr = Self::from_image(rhs)?;
        lhs_expr.binary_expr_with_name(rhs_expr, "zip", Arc::new(f))
    }

    /// Applies a built-in unary operator lazily.
    pub fn unary(self, op: ImageExprUnaryOp) -> Self {
        Self {
            node: NumericExprNode::UnaryOp {
                op,
                child: Box::new(self.node),
            },
            meta: self.meta,
            expr_string: None,
        }
    }

    /// Negates the expression lazily.
    pub fn negate(self) -> Self {
        self.unary(ImageExprUnaryOp::Negate)
    }

    /// Applies `exp(...)` lazily.
    pub fn exp(self) -> Self {
        self.unary(ImageExprUnaryOp::Exp)
    }

    /// Applies `sin(...)` lazily.
    pub fn sin(self) -> Self {
        self.unary(ImageExprUnaryOp::Sin)
    }

    /// Applies `cos(...)` lazily.
    pub fn cos(self) -> Self {
        self.unary(ImageExprUnaryOp::Cos)
    }

    /// Applies `tan(...)` lazily.
    pub fn tan(self) -> Self {
        self.unary(ImageExprUnaryOp::Tan)
    }

    /// Applies `asin(...)` lazily.
    pub fn asin(self) -> Self {
        self.unary(ImageExprUnaryOp::Asin)
    }

    /// Applies `acos(...)` lazily.
    pub fn acos(self) -> Self {
        self.unary(ImageExprUnaryOp::Acos)
    }

    /// Applies `atan(...)` lazily.
    pub fn atan(self) -> Self {
        self.unary(ImageExprUnaryOp::Atan)
    }

    /// Applies `sinh(...)` lazily.
    pub fn sinh(self) -> Self {
        self.unary(ImageExprUnaryOp::Sinh)
    }

    /// Applies `cosh(...)` lazily.
    pub fn cosh(self) -> Self {
        self.unary(ImageExprUnaryOp::Cosh)
    }

    /// Applies `tanh(...)` lazily.
    pub fn tanh(self) -> Self {
        self.unary(ImageExprUnaryOp::Tanh)
    }

    /// Applies `log(...)` (natural logarithm) lazily.
    pub fn log(self) -> Self {
        self.unary(ImageExprUnaryOp::Log)
    }

    /// Applies `log10(...)` lazily.
    pub fn log10(self) -> Self {
        self.unary(ImageExprUnaryOp::Log10)
    }

    /// Applies `sqrt(...)` lazily.
    pub fn sqrt(self) -> Self {
        self.unary(ImageExprUnaryOp::Sqrt)
    }

    /// Applies `abs(...)` lazily.  For complex types returns `Complex(|z|, 0)`.
    pub fn abs(self) -> Self {
        self.unary(ImageExprUnaryOp::Abs)
    }

    /// Applies `ceil(...)` lazily.  Component-wise for complex.
    pub fn ceil(self) -> Self {
        self.unary(ImageExprUnaryOp::Ceil)
    }

    /// Applies `floor(...)` lazily.  Component-wise for complex.
    pub fn floor(self) -> Self {
        self.unary(ImageExprUnaryOp::Floor)
    }

    /// Applies `round(...)` lazily.  Component-wise for complex.
    pub fn round(self) -> Self {
        self.unary(ImageExprUnaryOp::Round)
    }

    /// Applies `sign(...)` lazily.  For complex: `z / |z|` (or zero).
    pub fn sign(self) -> Self {
        self.unary(ImageExprUnaryOp::Sign)
    }

    /// Applies complex conjugate lazily.  Identity for real types.
    pub fn conj(self) -> Self {
        self.unary(ImageExprUnaryOp::Conj)
    }

    /// Combines two expressions using a built-in binary operator.
    ///
    /// Scalar (0-D) operands broadcast to the shape of the other operand,
    /// matching C++ LEL semantics where reductions produce scalars.
    pub fn binary_expr(self, rhs: Self, op: ImageExprBinaryOp) -> Result<Self, ImageError> {
        let out_shape = broadcast_shapes(&self.meta.shape, &rhs.meta.shape)?;
        let mut meta = if self.meta.shape.is_empty() {
            rhs.meta
        } else {
            self.meta
        };
        meta.shape = out_shape;
        Ok(Self {
            node: NumericExprNode::BinaryOp {
                op,
                lhs: Box::new(self.node),
                rhs: Box::new(rhs.node),
            },
            meta,
            expr_string: None,
        })
    }

    fn binary_expr_with_name(
        self,
        rhs: Self,
        name: &'static str,
        func: BinaryExprFn<T>,
    ) -> Result<Self, ImageError> {
        let out_shape = broadcast_shapes(&self.meta.shape, &rhs.meta.shape)?;
        let mut result = self.custom_binary(rhs, name, func);
        result.meta.shape = out_shape;
        Ok(result)
    }

    /// Combines this expression with another source image using a built-in binary operator.
    pub fn binary_image<I>(self, rhs: &'a I, op: ImageExprBinaryOp) -> Result<Self, ImageError>
    where
        I: ImageInterface<T> + 'a,
    {
        self.binary_expr(Self::from_image(rhs)?, op)
    }

    /// Applies a built-in binary operator against a scalar, broadcasting the scalar across the image.
    pub fn binary_scalar(self, rhs: T, op: ImageExprBinaryOp) -> Self {
        Self {
            node: NumericExprNode::BinaryOp {
                op,
                lhs: Box::new(self.node),
                rhs: Box::new(NumericExprNode::Scalar(rhs)),
            },
            meta: self.meta,
            expr_string: None,
        }
    }

    /// Applies a built-in binary operator with a scalar on the left side.
    ///
    /// Unlike [`binary_scalar`](Self::binary_scalar) where the scalar is on the
    /// right, this puts the scalar on the left: `scalar op image`.  Correctly
    /// handles non-commutative operators (subtract, divide, pow) and preserves
    /// non-finite pixel values (Inf, NaN) in the image operand.
    pub fn scalar_left_binary(scalar: T, rhs: Self, op: ImageExprBinaryOp) -> Self {
        Self {
            node: NumericExprNode::BinaryOp {
                op,
                lhs: Box::new(NumericExprNode::Scalar(scalar)),
                rhs: Box::new(rhs.node),
            },
            meta: rhs.meta,
            expr_string: None,
        }
    }

    /// Adds another expression lazily.
    pub fn add_expr(self, rhs: Self) -> Result<Self, ImageError> {
        self.binary_expr(rhs, ImageExprBinaryOp::Add)
    }

    /// Adds another source image lazily.
    pub fn add_image<I>(self, rhs: &'a I) -> Result<Self, ImageError>
    where
        I: ImageInterface<T> + 'a,
    {
        self.binary_image(rhs, ImageExprBinaryOp::Add)
    }

    /// Adds a scalar lazily.
    pub fn add_scalar(self, rhs: T) -> Self {
        self.binary_scalar(rhs, ImageExprBinaryOp::Add)
    }

    /// Multiplies by a scalar lazily.
    pub fn multiply_scalar(self, rhs: T) -> Self {
        self.binary_scalar(rhs, ImageExprBinaryOp::Multiply)
    }

    /// Multiplies two expressions lazily.
    pub fn multiply_expr(self, rhs: Self) -> Result<Self, ImageError> {
        self.binary_expr(rhs, ImageExprBinaryOp::Multiply)
    }

    /// Multiplies with another source image lazily.
    pub fn multiply_image<I>(self, rhs: &'a I) -> Result<Self, ImageError>
    where
        I: ImageInterface<T> + 'a,
    {
        self.binary_image(rhs, ImageExprBinaryOp::Multiply)
    }

    /// Subtracts another expression lazily.
    pub fn subtract_expr(self, rhs: Self) -> Result<Self, ImageError> {
        self.binary_expr(rhs, ImageExprBinaryOp::Subtract)
    }

    /// Subtracts another source image lazily.
    pub fn subtract_image<I>(self, rhs: &'a I) -> Result<Self, ImageError>
    where
        I: ImageInterface<T> + 'a,
    {
        self.binary_image(rhs, ImageExprBinaryOp::Subtract)
    }

    /// Subtracts a scalar lazily.
    pub fn subtract_scalar(self, rhs: T) -> Self {
        self.binary_scalar(rhs, ImageExprBinaryOp::Subtract)
    }

    /// Divides by another expression lazily.
    pub fn divide_expr(self, rhs: Self) -> Result<Self, ImageError> {
        self.binary_expr(rhs, ImageExprBinaryOp::Divide)
    }

    /// Divides by another source image lazily.
    pub fn divide_image<I>(self, rhs: &'a I) -> Result<Self, ImageError>
    where
        I: ImageInterface<T> + 'a,
    {
        self.binary_image(rhs, ImageExprBinaryOp::Divide)
    }

    /// Divides by a scalar lazily.
    pub fn divide_scalar(self, rhs: T) -> Self {
        self.binary_scalar(rhs, ImageExprBinaryOp::Divide)
    }

    /// Raises each pixel to a scalar power lazily: `pow(x, scalar)`.
    pub fn pow_scalar(self, rhs: T) -> Self {
        self.binary_scalar(rhs, ImageExprBinaryOp::Pow)
    }

    /// Element-wise power with another expression: `pow(self, rhs)`.
    pub fn pow_expr(self, rhs: Self) -> Result<Self, ImageError> {
        self.binary_expr(rhs, ImageExprBinaryOp::Pow)
    }

    /// Element-wise power with another source image: `pow(self, rhs)`.
    pub fn pow_image<I>(self, rhs: &'a I) -> Result<Self, ImageError>
    where
        I: ImageInterface<T> + 'a,
    {
        self.binary_image(rhs, ImageExprBinaryOp::Pow)
    }

    /// Element-wise minimum with a scalar.
    pub fn min_scalar(self, rhs: T) -> Self {
        self.binary_scalar(rhs, ImageExprBinaryOp::Min)
    }

    /// Element-wise minimum with another expression.
    pub fn min_expr(self, rhs: Self) -> Result<Self, ImageError> {
        self.binary_expr(rhs, ImageExprBinaryOp::Min)
    }

    /// Element-wise minimum with another source image.
    pub fn min_image<I>(self, rhs: &'a I) -> Result<Self, ImageError>
    where
        I: ImageInterface<T> + 'a,
    {
        self.binary_image(rhs, ImageExprBinaryOp::Min)
    }

    /// Element-wise maximum with a scalar.
    pub fn max_scalar(self, rhs: T) -> Self {
        self.binary_scalar(rhs, ImageExprBinaryOp::Max)
    }

    /// Element-wise maximum with another expression.
    pub fn max_expr(self, rhs: Self) -> Result<Self, ImageError> {
        self.binary_expr(rhs, ImageExprBinaryOp::Max)
    }

    /// Element-wise maximum with another source image.
    pub fn max_image<I>(self, rhs: &'a I) -> Result<Self, ImageError>
    where
        I: ImageInterface<T> + 'a,
    {
        self.binary_image(rhs, ImageExprBinaryOp::Max)
    }

    /// Floating-point remainder with a scalar: `fmod(x, scalar)`.
    pub fn fmod_scalar(self, rhs: T) -> Self {
        self.binary_scalar(rhs, ImageExprBinaryOp::Fmod)
    }

    /// Element-wise floating-point remainder with another expression.
    pub fn fmod_expr(self, rhs: Self) -> Result<Self, ImageError> {
        self.binary_expr(rhs, ImageExprBinaryOp::Fmod)
    }

    /// Element-wise floating-point remainder with another source image.
    pub fn fmod_image<I>(self, rhs: &'a I) -> Result<Self, ImageError>
    where
        I: ImageInterface<T> + 'a,
    {
        self.binary_image(rhs, ImageExprBinaryOp::Fmod)
    }

    /// Two-argument arc tangent with a scalar: `atan2(self, scalar)`.
    pub fn atan2_scalar(self, rhs: T) -> Self {
        self.binary_scalar(rhs, ImageExprBinaryOp::Atan2)
    }

    /// Two-argument arc tangent with another expression: `atan2(self, rhs)`.
    pub fn atan2_expr(self, rhs: Self) -> Result<Self, ImageError> {
        self.binary_expr(rhs, ImageExprBinaryOp::Atan2)
    }

    /// Two-argument arc tangent with another source image: `atan2(self, rhs)`.
    pub fn atan2_image<I>(self, rhs: &'a I) -> Result<Self, ImageError>
    where
        I: ImageInterface<T> + 'a,
    {
        self.binary_image(rhs, ImageExprBinaryOp::Atan2)
    }

    /// Reads the full evaluated array.
    pub fn get(&self) -> Result<ArrayD<T>, ImageError> {
        <Self as Lattice<T>>::get(self).map_err(Into::into)
    }

    /// Reads a single pixel from the expression.
    pub fn get_at(&self, position: &[usize]) -> Result<T, ImageError> {
        <Self as Lattice<T>>::get_at(self, position).map_err(Into::into)
    }

    /// Reads a unit-stride slice from the expression.
    pub fn get_slice(&self, start: &[usize], shape: &[usize]) -> Result<ArrayD<T>, ImageError> {
        <Self as Lattice<T>>::get_slice(self, start, shape, &vec![1; self.ndim()])
            .map_err(Into::into)
    }

    /// Reads a strided slice from the expression.
    pub fn get_slice_with_stride(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, ImageError> {
        <Self as Lattice<T>>::get_slice(self, start, shape, stride).map_err(Into::into)
    }

    /// Comparison helper producing a lazy boolean mask expression.
    pub fn compare_scalar(self, scalar: T, op: ImageExprCompareOp) -> MaskExpr<'a, T>
    where
        T: PartialOrd,
    {
        MaskExpr {
            node: MaskExprNode::CompareScalar {
                op,
                expr: Box::new(self.node),
                scalar,
            },
            shape: self.meta.shape,
        }
    }

    /// Convenience `>` comparison against a scalar.
    pub fn gt_scalar(self, scalar: T) -> MaskExpr<'a, T>
    where
        T: PartialOrd,
    {
        self.compare_scalar(scalar, ImageExprCompareOp::GreaterThan)
    }

    /// Convenience `<` comparison against a scalar.
    pub fn lt_scalar(self, scalar: T) -> MaskExpr<'a, T>
    where
        T: PartialOrd,
    {
        self.compare_scalar(scalar, ImageExprCompareOp::LessThan)
    }

    /// Convenience `>=` comparison against a scalar.
    pub fn ge_scalar(self, scalar: T) -> MaskExpr<'a, T>
    where
        T: PartialOrd,
    {
        self.compare_scalar(scalar, ImageExprCompareOp::GreaterEqual)
    }

    /// Convenience `<=` comparison against a scalar.
    pub fn le_scalar(self, scalar: T) -> MaskExpr<'a, T>
    where
        T: PartialOrd,
    {
        self.compare_scalar(scalar, ImageExprCompareOp::LessEqual)
    }

    /// Convenience `==` comparison against a scalar.
    pub fn eq_scalar(self, scalar: T) -> MaskExpr<'a, T>
    where
        T: PartialOrd,
    {
        self.compare_scalar(scalar, ImageExprCompareOp::Equal)
    }

    /// Convenience `!=` comparison against a scalar.
    pub fn ne_scalar(self, scalar: T) -> MaskExpr<'a, T>
    where
        T: PartialOrd,
    {
        self.compare_scalar(scalar, ImageExprCompareOp::NotEqual)
    }

    // -- Wave 14a: isnan, metadata queries --

    /// Element-wise NaN test producing a boolean mask.
    ///
    /// Corresponds to C++ LEL `isnan(expr)`.
    pub fn isnan(self) -> MaskExpr<'a, T>
    where
        T: PartialOrd,
    {
        MaskExpr {
            shape: self.meta.shape.clone(),
            node: MaskExprNode::IsNan {
                child: Box::new(self.node),
            },
        }
    }

    /// Returns the propagated default pixel mask when it can be derived from
    /// the numeric DAG.
    ///
    /// Direct image references return their on-disk default pixel mask.
    /// Built-in unary/binary numeric nodes propagate that mask through the
    /// expression tree, treating scalars as all-true and AND-ing image masks.
    /// Opaque typed closures and some non-elementwise nodes still return
    /// `None`, which callers treat as an all-true fallback.
    pub fn source_mask(&self) -> Result<Option<ArrayD<bool>>, ImageError> {
        self.node.propagated_mask()
    }

    /// Returns the number of dimensions of this expression.
    ///
    /// Corresponds to C++ LEL `ndim(expr)`.
    pub fn ndim_value(&self) -> usize {
        self.meta.shape.len()
    }

    /// Returns the total number of elements in this expression.
    ///
    /// Corresponds to C++ LEL `nelem(expr)`.
    pub fn nelem_value(&self) -> usize {
        self.meta.shape.iter().product()
    }

    /// Returns the length of a specific axis.
    ///
    /// Corresponds to C++ LEL `length(expr, axis)`.
    pub fn length_value(&self, axis: usize) -> Option<usize> {
        self.meta.shape.get(axis).copied()
    }

    // -- Wave 14b: reduction methods --

    /// Reduces the entire expression to a scalar (0-D) sum.
    ///
    /// Matches C++ LEL `sum(expr)` which produces a scalar lattice.
    /// When combined with array expressions via binary ops, the scalar
    /// broadcasts to the array shape automatically.
    pub fn sum_reduce(self) -> Self {
        self.make_reduction(ReductionOp::Sum)
    }

    /// Reduces the entire expression to a scalar (0-D) minimum.
    ///
    /// Corresponds to C++ LEL `min1d(expr)`.
    pub fn min_reduce(self) -> Self {
        self.make_reduction(ReductionOp::Min)
    }

    /// Reduces the entire expression to a scalar (0-D) maximum.
    ///
    /// Corresponds to C++ LEL `max1d(expr)`.
    pub fn max_reduce(self) -> Self {
        self.make_reduction(ReductionOp::Max)
    }

    /// Reduces the entire expression to a scalar (0-D) mean.
    ///
    /// Corresponds to C++ LEL `mean1d(expr)`.
    pub fn mean_reduce(self) -> Self {
        self.make_reduction(ReductionOp::Mean)
    }

    /// Reduces the entire expression to a scalar (0-D) median.
    ///
    /// Corresponds to C++ LEL `median1d(expr)`.
    pub fn median_reduce(self) -> Self {
        self.make_reduction(ReductionOp::Median)
    }

    fn make_reduction(self, op: ReductionOp) -> Self {
        let child_shape = self.meta.shape.clone();
        let mut meta = self.meta;
        meta.shape = vec![];
        Self {
            node: NumericExprNode::Reduction {
                op,
                child: Box::new(self.node),
                child_shape,
            },
            meta,
            expr_string: None,
        }
    }

    /// Computes the fractile (quantile) of all elements as a scalar (0-D).
    ///
    /// Corresponds to C++ LEL `fractile1d(expr, fraction)`.
    pub fn fractile(self, fraction: f64) -> Self {
        let child_shape = self.meta.shape.clone();
        let mut meta = self.meta;
        meta.shape = vec![];
        Self {
            node: NumericExprNode::Fractile {
                child: Box::new(self.node),
                child_shape,
                fraction,
            },
            meta,
            expr_string: None,
        }
    }

    /// Computes the range between two fractiles as a scalar (0-D).
    ///
    /// Corresponds to C++ LEL `fractilerange1d(expr, frac1, frac2)`.
    pub fn fractile_range(self, fraction1: f64, fraction2: f64) -> Self {
        let child_shape = self.meta.shape.clone();
        let mut meta = self.meta;
        meta.shape = vec![];
        Self {
            node: NumericExprNode::FractileRange {
                child: Box::new(self.node),
                child_shape,
                fraction1,
                fraction2,
            },
            meta,
            expr_string: None,
        }
    }

    // -- Wave 14c: conditional, mask interaction --

    /// Conditional expression: `iif(condition, true_val, false_val)`.
    ///
    /// Where the condition mask is `true`, returns `if_true` pixels; otherwise
    /// returns `if_false` pixels.
    ///
    /// Corresponds to C++ LEL `iif(cond, true, false)`.
    pub fn iif(
        condition: MaskExpr<'a, T>,
        if_true: Self,
        if_false: Self,
    ) -> Result<Self, ImageError>
    where
        T: PartialOrd,
    {
        let out_shape = broadcast_shapes(&condition.shape, &if_true.meta.shape)?;
        let out_shape = broadcast_shapes(&out_shape, &if_false.meta.shape)?;
        let cond_node = condition.node;
        let mut meta = if !if_true.meta.shape.is_empty() {
            if_true.meta.clone()
        } else if !if_false.meta.shape.is_empty() {
            if_false.meta.clone()
        } else {
            if_true.meta.clone()
        };
        meta.shape = out_shape;
        Ok(Self {
            node: NumericExprNode::Conditional {
                condition: Box::new(cond_node),
                if_true: Box::new(if_true.node),
                if_false: Box::new(if_false.node),
            },
            meta,
            expr_string: None,
        })
    }

    /// Counts true values in a boolean mask, returning a scalar (0-D) count.
    ///
    /// Corresponds to C++ LEL `ntrue(mask)`.
    pub fn ntrue(mask: MaskExpr<'a, T>) -> Self
    where
        T: PartialOrd,
    {
        Self::mask_count(mask, true)
    }

    /// Counts false values in a boolean mask, returning a scalar (0-D) count.
    ///
    /// Corresponds to C++ LEL `nfalse(mask)`.
    pub fn nfalse(mask: MaskExpr<'a, T>) -> Self
    where
        T: PartialOrd,
    {
        Self::mask_count(mask, false)
    }

    fn mask_count(mask: MaskExpr<'a, T>, count_true: bool) -> Self
    where
        T: PartialOrd,
    {
        let mask_shape = mask.shape.clone();
        let mask_node = mask.node;
        Self {
            node: NumericExprNode::MaskCount {
                count_true,
                mask: Box::new(mask_node),
                mask_shape,
            },
            meta: ImageExprMeta {
                shape: vec![],
                coords: CoordinateSystem::new(),
                units: String::new(),
                misc_info: casacore_types::RecordValue::default(),
                image_info: ImageInfo::default(),
                name: None,
            },
            expr_string: None,
        }
    }

    /// Replace pixels where the mask is false with values from the replacement.
    ///
    /// Corresponds to C++ LEL `replace(expr, replacement)`.
    ///
    /// `replacement` may be same-shaped or scalar; scalar replacements
    /// broadcast across the primary expression.
    pub fn replace(self, replacement: Self, mask: ArrayD<bool>) -> Result<Self, ImageError> {
        let out_shape = broadcast_shapes(&self.meta.shape, &replacement.meta.shape)?;
        if out_shape != self.meta.shape {
            return Err(ImageError::ShapeMismatch {
                expected: self.meta.shape.clone(),
                got: replacement.meta.shape.clone(),
            });
        }
        Ok(Self {
            node: NumericExprNode::Replace {
                primary: Box::new(self.node),
                replacement: Box::new(replacement.node),
                mask,
            },
            meta: self.meta,
            expr_string: None,
        })
    }

    /// Attempts to mutate a read-only expression, returning an explicit error.
    pub fn put_at(&mut self, _value: T, _position: &[usize]) -> Result<(), ImageError> {
        Err(ImageError::ReadOnly("ImageExpr"))
    }

    /// Attempts to mutate a read-only expression, returning an explicit error.
    pub fn put_slice(&mut self, _data: &ArrayD<T>, _start: &[usize]) -> Result<(), ImageError> {
        Err(ImageError::ReadOnly("ImageExpr"))
    }

    /// Attempts to mutate a read-only expression, returning an explicit error.
    pub fn set(&mut self, _value: T) -> Result<(), ImageError> {
        Err(ImageError::ReadOnly("ImageExpr"))
    }

    /// Persists the evaluated expression as a new paged image.
    ///
    /// This compiles the borrowed expression into an owned execution form,
    /// then materializes it chunk by chunk as a regular `PagedImage` table.
    /// The resulting image is fully independent of the expression sources.
    pub fn save_as(&self, path: impl AsRef<Path>) -> Result<PagedImage<T>, ImageError> {
        self.compile()?.save_as(path)
    }

    /// Persists the expression string in casacore-compatible `.imgexpr` format.
    ///
    /// Creates a directory at `path` containing `imageexpr.json` with the
    /// expression string, pixel data type, and miscellaneous metadata.  The
    /// source images referenced in the expression must remain accessible at
    /// the paths embedded in the expression string for any future reopen
    /// (by Rust or C++) to succeed.
    ///
    /// This is the Rust analogue of C++ `ImageExpr<T>::save()`.
    ///
    /// # Errors
    ///
    /// Returns an error if no expression string has been set (i.e. the
    /// expression was built programmatically without [`Self::set_expr_string`]),
    /// or if directory / file creation fails.
    pub fn save_expr(&self, path: impl AsRef<Path>) -> Result<(), ImageError> {
        let expr_str = self.expr_string.as_deref().ok_or_else(|| {
            ImageError::InvalidMetadata(
                "ImageExpr cannot be persisted: no expression string is set".into(),
            )
        })?;
        crate::expr_file::save(path, expr_str, T::PRIMITIVE_TYPE, &self.meta.misc_info)
    }

    /// Returns the expression string, if one was set by the parser or
    /// [`set_expr_string`](Self::set_expr_string).
    pub fn expr_string(&self) -> Option<&str> {
        self.expr_string.as_deref()
    }

    /// Attaches a LEL expression string to this expression.
    ///
    /// This is required before calling [`save_expr`](Self::save_expr).
    /// The parser sets this automatically; users building expressions
    /// programmatically must call this with the corresponding LEL string
    /// if they want expression-preserving persistence.
    pub fn set_expr_string(&mut self, expr: impl Into<String>) {
        self.expr_string = Some(expr.into());
    }
}

// -- Wave 14d: Type-changing methods (typed API only) --

impl<'a> ImageExpr<'a, casacore_types::Complex32> {
    /// Extracts the real part of a Complex32 expression, producing an f32 expression.
    ///
    /// Corresponds to C++ LEL `real(expr)`.
    pub fn real_part(self) -> ImageExpr<'a, f32> {
        let node = self.node;
        let meta = self.meta;
        ImageExpr {
            node: NumericExprNode::TypeBridge {
                eval_fn: Arc::new(move |start, shape, stride| {
                    let data = node.eval_slice(start, shape, stride)?;
                    Ok(data.mapv(|v| v.re))
                }),
            },
            meta: ImageExprMeta {
                shape: meta.shape,
                coords: meta.coords,
                units: meta.units,
                misc_info: meta.misc_info,
                image_info: meta.image_info,
                name: meta.name,
            },
            expr_string: None,
        }
    }

    /// Extracts the imaginary part of a Complex32 expression, producing an f32 expression.
    ///
    /// Corresponds to C++ LEL `imag(expr)`.
    pub fn imag_part(self) -> ImageExpr<'a, f32> {
        let node = self.node;
        let meta = self.meta;
        ImageExpr {
            node: NumericExprNode::TypeBridge {
                eval_fn: Arc::new(move |start, shape, stride| {
                    let data = node.eval_slice(start, shape, stride)?;
                    Ok(data.mapv(|v| v.im))
                }),
            },
            meta: ImageExprMeta {
                shape: meta.shape,
                coords: meta.coords,
                units: meta.units,
                misc_info: meta.misc_info,
                image_info: meta.image_info,
                name: meta.name,
            },
            expr_string: None,
        }
    }

    /// Extracts the argument (phase angle) of a Complex32 expression, producing an f32 expression.
    ///
    /// Corresponds to C++ LEL `arg(expr)` = `atan2(im, re)`.
    pub fn arg_phase(self) -> ImageExpr<'a, f32> {
        let node = self.node;
        let meta = self.meta;
        ImageExpr {
            node: NumericExprNode::TypeBridge {
                eval_fn: Arc::new(move |start, shape, stride| {
                    let data = node.eval_slice(start, shape, stride)?;
                    Ok(data.mapv(|v| v.im.atan2(v.re)))
                }),
            },
            meta: ImageExprMeta {
                shape: meta.shape,
                coords: meta.coords,
                units: meta.units,
                misc_info: meta.misc_info,
                image_info: meta.image_info,
                name: meta.name,
            },
            expr_string: None,
        }
    }
}

impl<'a> ImageExpr<'a, casacore_types::Complex64> {
    /// Extracts the real part of a Complex64 expression, producing an f64 expression.
    pub fn real_part(self) -> ImageExpr<'a, f64> {
        let node = self.node;
        let meta = self.meta;
        ImageExpr {
            node: NumericExprNode::TypeBridge {
                eval_fn: Arc::new(move |start, shape, stride| {
                    let data = node.eval_slice(start, shape, stride)?;
                    Ok(data.mapv(|v| v.re))
                }),
            },
            meta: ImageExprMeta {
                shape: meta.shape,
                coords: meta.coords,
                units: meta.units,
                misc_info: meta.misc_info,
                image_info: meta.image_info,
                name: meta.name,
            },
            expr_string: None,
        }
    }

    /// Extracts the imaginary part of a Complex64 expression, producing an f64 expression.
    pub fn imag_part(self) -> ImageExpr<'a, f64> {
        let node = self.node;
        let meta = self.meta;
        ImageExpr {
            node: NumericExprNode::TypeBridge {
                eval_fn: Arc::new(move |start, shape, stride| {
                    let data = node.eval_slice(start, shape, stride)?;
                    Ok(data.mapv(|v| v.im))
                }),
            },
            meta: ImageExprMeta {
                shape: meta.shape,
                coords: meta.coords,
                units: meta.units,
                misc_info: meta.misc_info,
                image_info: meta.image_info,
                name: meta.name,
            },
            expr_string: None,
        }
    }

    /// Extracts the argument (phase angle) of a Complex64 expression, producing an f64 expression.
    pub fn arg_phase(self) -> ImageExpr<'a, f64> {
        let node = self.node;
        let meta = self.meta;
        ImageExpr {
            node: NumericExprNode::TypeBridge {
                eval_fn: Arc::new(move |start, shape, stride| {
                    let data = node.eval_slice(start, shape, stride)?;
                    Ok(data.mapv(|v| v.im.atan2(v.re)))
                }),
            },
            meta: ImageExprMeta {
                shape: meta.shape,
                coords: meta.coords,
                units: meta.units,
                misc_info: meta.misc_info,
                image_info: meta.image_info,
                name: meta.name,
            },
            expr_string: None,
        }
    }
}

impl<'a> ImageExpr<'a, f32> {
    /// Combines two f32 expressions into a Complex32 expression.
    ///
    /// Corresponds to C++ LEL `complex(real, imag)`.
    pub fn to_complex(
        self,
        imag: Self,
    ) -> Result<ImageExpr<'a, casacore_types::Complex32>, ImageError> {
        validate_same_shape(self.shape(), imag.shape())?;
        let real_node = self.node;
        let imag_node = imag.node;
        let meta = self.meta;
        Ok(ImageExpr {
            node: NumericExprNode::TypeBridge {
                eval_fn: Arc::new(move |start, shape, stride| {
                    let re = real_node.eval_slice(start, shape, stride)?;
                    let im = imag_node.eval_slice(start, shape, stride)?;
                    Ok(Zip::from(&re)
                        .and(&im)
                        .map_collect(|&re, &im| casacore_types::Complex32::new(re, im)))
                }),
            },
            meta: ImageExprMeta {
                shape: meta.shape,
                coords: meta.coords,
                units: meta.units,
                misc_info: meta.misc_info,
                image_info: meta.image_info,
                name: meta.name,
            },
            expr_string: None,
        })
    }
}

impl<'a> ImageExpr<'a, f64> {
    /// Combines two f64 expressions into a Complex64 expression.
    pub fn to_complex(
        self,
        imag: Self,
    ) -> Result<ImageExpr<'a, casacore_types::Complex64>, ImageError> {
        validate_same_shape(self.shape(), imag.shape())?;
        let real_node = self.node;
        let imag_node = imag.node;
        let meta = self.meta;
        Ok(ImageExpr {
            node: NumericExprNode::TypeBridge {
                eval_fn: Arc::new(move |start, shape, stride| {
                    let re = real_node.eval_slice(start, shape, stride)?;
                    let im = imag_node.eval_slice(start, shape, stride)?;
                    Ok(Zip::from(&re)
                        .and(&im)
                        .map_collect(|&re, &im| casacore_types::Complex64::new(re, im)))
                }),
            },
            meta: ImageExprMeta {
                shape: meta.shape,
                coords: meta.coords,
                units: meta.units,
                misc_info: meta.misc_info,
                image_info: meta.image_info,
                name: meta.name,
            },
            expr_string: None,
        })
    }
}

impl<'a, T: ImageExprValue> Lattice<T> for ImageExpr<'a, T> {
    fn shape(&self) -> &[usize] {
        &self.meta.shape
    }

    fn is_writable(&self) -> bool {
        false
    }

    fn get_at(&self, position: &[usize]) -> Result<T, LatticeError> {
        validate_slice_request(
            self.shape(),
            position,
            &vec![1; self.ndim()],
            &vec![1; self.ndim()],
        )?;
        let one = self
            .node
            .eval_slice(position, &vec![1; self.ndim()], &vec![1; self.ndim()])?;
        Ok(one[IxDyn(&vec![0; self.ndim()])])
    }

    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError> {
        validate_slice_request(self.shape(), start, shape, stride)?;
        self.node.eval_slice(start, shape, stride)
    }

    fn get(&self) -> Result<ArrayD<T>, LatticeError> {
        if self.ndim() == 0 {
            return self.node.eval_slice(&[], &[], &[]);
        }

        let full_shape = self.shape().to_vec();
        let cursor_shape = self.node.preferred_cursor_shape(&full_shape);
        try_fold_traversal_cursors(
            &full_shape,
            &cursor_shape,
            TraversalSpec::chunks(cursor_shape.clone()),
            ArrayD::from_elem(IxDyn(&full_shape), T::default_value()),
            |out, cursor| {
                let stride = vec![1; cursor.position.len()];
                let chunk = self
                    .node
                    .eval_slice(&cursor.position, &cursor.shape, &stride)?;
                write_chunk_into_array(out, &cursor.position, &chunk)
            },
        )
    }
}

impl<'a, T: ImageExprValue> ImageInterface<T> for ImageExpr<'a, T> {
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        None
    }

    fn coordinates(&self) -> &CoordinateSystem {
        &self.meta.coords
    }

    fn units(&self) -> &str {
        &self.meta.units
    }

    fn misc_info(&self) -> casacore_types::RecordValue {
        self.meta.misc_info.clone()
    }

    fn image_info(&self) -> Result<ImageInfo, ImageError> {
        Ok(self.meta.image_info.clone())
    }

    fn name(&self) -> Option<&Path> {
        self.meta.name.as_deref()
    }
}

/// Lazy boolean mask expression built from numeric image expressions.
pub struct MaskExpr<'a, T: ImageExprValue> {
    node: MaskExprNode<'a, T>,
    shape: Vec<usize>,
}

impl<'a, T: ImageExprValue> Clone for MaskExpr<'a, T> {
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            shape: self.shape.clone(),
        }
    }
}

impl<'a, T: ImageExprValue> fmt::Debug for MaskExpr<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaskExpr")
            .field("shape", &self.shape)
            .finish()
    }
}

impl<'a, T: ImageExprValue + PartialOrd> MaskExpr<'a, T> {
    /// Reads the full evaluated boolean mask.
    pub fn get(&self) -> Result<ArrayD<bool>, ImageError> {
        <Self as Lattice<bool>>::get(self).map_err(Into::into)
    }

    /// Reads a single mask element.
    pub fn get_at(&self, position: &[usize]) -> Result<bool, ImageError> {
        <Self as Lattice<bool>>::get_at(self, position).map_err(Into::into)
    }

    /// Reads a unit-stride slice of the mask.
    pub fn get_slice(&self, start: &[usize], shape: &[usize]) -> Result<ArrayD<bool>, ImageError> {
        <Self as Lattice<bool>>::get_slice(self, start, shape, &vec![1; self.ndim()])
            .map_err(Into::into)
    }

    /// Combines two same-shaped masks with a logical operator.
    pub fn logical(self, rhs: Self, op: MaskLogicalOp) -> Result<Self, ImageError> {
        let out_shape = broadcast_shapes(&self.shape, &rhs.shape)?;
        Ok(Self {
            node: MaskExprNode::Logical {
                op,
                lhs: Box::new(self.node),
                rhs: Box::new(rhs.node),
            },
            shape: out_shape,
        })
    }

    /// Boolean AND with another mask expression.
    pub fn and(self, rhs: Self) -> Result<Self, ImageError> {
        self.logical(rhs, MaskLogicalOp::And)
    }

    /// Boolean OR with another mask expression.
    pub fn or(self, rhs: Self) -> Result<Self, ImageError> {
        self.logical(rhs, MaskLogicalOp::Or)
    }

    /// Boolean negation.
    pub fn logical_not(self) -> Self {
        Self {
            node: MaskExprNode::Not {
                child: Box::new(self.node),
            },
            shape: self.shape,
        }
    }

    /// Creates a constant mask from an existing boolean array.
    ///
    /// Corresponds to C++ LEL `mask(image)` — the image's default pixel mask
    /// is pre-read and stored as a constant node.
    pub fn from_constant(mask: ArrayD<bool>) -> Self {
        let shape = mask.shape().to_vec();
        Self {
            node: MaskExprNode::ConstantMask { mask },
            shape,
        }
    }

    /// Reduces this mask to a scalar (0-D) boolean: `true` iff all are `true`.
    ///
    /// Corresponds to C++ LEL `all(mask)`.
    pub fn all_reduce(self) -> Self {
        let child_shape = self.shape;
        Self {
            node: MaskExprNode::AllReduce {
                child: Box::new(self.node),
                child_shape,
            },
            shape: vec![],
        }
    }

    /// Reduces this mask to a scalar (0-D) boolean: `true` iff any is `true`.
    ///
    /// Corresponds to C++ LEL `any(mask)`.
    pub fn any_reduce(self) -> Self {
        let child_shape = self.shape;
        Self {
            node: MaskExprNode::AnyReduce {
                child: Box::new(self.node),
                child_shape,
            },
            shape: vec![],
        }
    }
}

impl<'a, T: ImageExprValue + PartialOrd> Lattice<bool> for MaskExpr<'a, T> {
    fn shape(&self) -> &[usize] {
        &self.shape
    }

    fn is_writable(&self) -> bool {
        false
    }

    fn get_at(&self, position: &[usize]) -> Result<bool, LatticeError> {
        validate_slice_request(
            self.shape(),
            position,
            &vec![1; self.ndim()],
            &vec![1; self.ndim()],
        )?;
        let one = self
            .node
            .eval_slice(position, &vec![1; self.ndim()], &vec![1; self.ndim()])?;
        Ok(one[IxDyn(&vec![0; self.ndim()])])
    }

    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<bool>, LatticeError> {
        validate_slice_request(self.shape(), start, shape, stride)?;
        self.node.eval_slice(start, shape, stride)
    }

    fn get(&self) -> Result<ArrayD<bool>, LatticeError> {
        if self.ndim() == 0 {
            return self.node.eval_slice(&[], &[], &[]);
        }

        let full_shape = self.shape().to_vec();
        let cursor_shape = self.node.preferred_cursor_shape(&full_shape);
        try_fold_traversal_cursors(
            &full_shape,
            &cursor_shape,
            TraversalSpec::chunks(cursor_shape.clone()),
            ArrayD::from_elem(IxDyn(&full_shape), false),
            |out, cursor| {
                let stride = vec![1; cursor.position.len()];
                let chunk = self
                    .node
                    .eval_slice(&cursor.position, &cursor.shape, &stride)?;
                write_chunk_into_array(out, &cursor.position, &chunk)
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::*;
    use casacore_coordinates::CoordinateSystem;
    use casacore_types::{Complex32, Complex64, RecordValue};
    use ndarray::{Dimension, IxDyn};

    struct CountingImage<T: ImageExprValue> {
        data: ArrayD<T>,
        coords: CoordinateSystem,
        cursor_shape: Option<Vec<usize>>,
        slice_shapes: Rc<RefCell<Vec<Vec<usize>>>>,
    }

    impl<T: ImageExprValue> CountingImage<T> {
        fn new(data: ArrayD<T>) -> (Self, Rc<RefCell<Vec<Vec<usize>>>>) {
            Self::new_with_cursor_shape(data, None)
        }

        fn new_with_cursor_shape(
            data: ArrayD<T>,
            cursor_shape: Option<Vec<usize>>,
        ) -> (Self, Rc<RefCell<Vec<Vec<usize>>>>) {
            let slice_shapes = Rc::new(RefCell::new(Vec::new()));
            (
                Self {
                    data,
                    coords: CoordinateSystem::new(),
                    cursor_shape,
                    slice_shapes: Rc::clone(&slice_shapes),
                },
                slice_shapes,
            )
        }
    }

    impl<T: ImageExprValue> Lattice<T> for CountingImage<T> {
        fn shape(&self) -> &[usize] {
            self.data.shape()
        }

        fn get_at(&self, position: &[usize]) -> Result<T, LatticeError> {
            self.data
                .get(IxDyn(position))
                .cloned()
                .ok_or_else(|| LatticeError::IndexOutOfBounds {
                    index: position.to_vec(),
                    shape: self.data.shape().to_vec(),
                })
        }

        fn get_slice(
            &self,
            start: &[usize],
            shape: &[usize],
            stride: &[usize],
        ) -> Result<ArrayD<T>, LatticeError> {
            self.slice_shapes.borrow_mut().push(shape.to_vec());
            let mut out = ArrayD::from_elem(IxDyn(shape), T::default_value());
            for (idx, value) in out.indexed_iter_mut() {
                let src: Vec<usize> = idx
                    .slice()
                    .iter()
                    .zip(start.iter())
                    .zip(stride.iter())
                    .map(|((&i, &s), &st)| s + i * st)
                    .collect();
                *value = self.get_at(&src)?;
            }
            Ok(out)
        }

        fn nice_cursor_shape(&self) -> Vec<usize> {
            self.cursor_shape
                .clone()
                .unwrap_or_else(|| self.data.shape().to_vec())
        }
    }

    impl<T: ImageExprValue> ImageInterface<T> for CountingImage<T> {
        fn as_any(&self) -> Option<&dyn std::any::Any> {
            None
        }

        fn coordinates(&self) -> &CoordinateSystem {
            &self.coords
        }

        fn units(&self) -> &str {
            ""
        }

        fn misc_info(&self) -> casacore_types::RecordValue {
            RecordValue::default()
        }

        fn image_info(&self) -> Result<ImageInfo, ImageError> {
            Ok(ImageInfo::default())
        }

        fn name(&self) -> Option<&Path> {
            None
        }
    }

    fn make_persistent_image<T: ImageExprValue>(shape: Vec<usize>) -> crate::TempImage<T> {
        crate::TempImage::new(shape, CoordinateSystem::new()).unwrap()
    }

    #[test]
    fn reduction_reads_source_in_chunks() {
        let shape = vec![4097, 1024];
        let data = ArrayD::from_elem(IxDyn(&shape), 1.0f32);
        let (image, slice_shapes) =
            CountingImage::new_with_cursor_shape(data, Some(vec![256, 128]));
        let expr = ImageExpr::from_image(&image)
            .unwrap()
            .add_scalar(0.0)
            .sum_reduce();

        let got = expr.get().unwrap();
        assert_eq!(got[IxDyn(&[])], (shape[0] * shape[1]) as f32);

        let seen = slice_shapes.borrow();
        assert!(seen.len() > 1);
        assert!(seen.iter().all(|shape| shape[0] <= 256 && shape[1] <= 128));
    }

    #[test]
    fn representative_lazy_slice_stays_local() {
        let data = ArrayD::from_shape_fn(IxDyn(&[8, 8]), |idx| (idx[0] * 10 + idx[1]) as f32);
        let (image, slice_shapes) = CountingImage::new(data);

        let expr = ImageExpr::from_image(&image).unwrap().add_scalar(2.0).exp();
        let slice = expr.get_slice(&[3, 4], &[1, 2]).unwrap();

        assert_eq!(slice.shape(), &[1, 2]);
        let recorded = slice_shapes.borrow();
        assert!(!recorded.is_empty());
        assert!(recorded.iter().all(|shape| shape == &vec![1, 2]));
    }

    #[test]
    fn image_and_scalar_ops_are_lazy_and_correct() {
        let mut lhs = make_persistent_image::<f32>(vec![2, 2]);
        let mut rhs = make_persistent_image::<f32>(vec![2, 2]);
        lhs.put_slice(
            &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0, 2.0, 3.0, 4.0]).unwrap(),
            &[0, 0],
        )
        .unwrap();
        rhs.put_slice(
            &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![0.5, 1.0, 1.5, 2.0]).unwrap(),
            &[0, 0],
        )
        .unwrap();

        let expr = ImageExpr::from_image(&lhs)
            .unwrap()
            .add_image(&rhs)
            .unwrap()
            .multiply_scalar(2.0)
            .negate();

        let got = expr.get().unwrap();
        let expected =
            ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![-3.0, -6.0, -9.0, -12.0]).unwrap();
        assert_eq!(got, expected);
    }

    #[test]
    fn complex_ops_work_without_materializing_sources() {
        let mut lhs = make_persistent_image::<Complex32>(vec![2, 2]);
        let mut rhs = make_persistent_image::<Complex32>(vec![2, 2]);
        lhs.put_slice(
            &ArrayD::from_shape_vec(
                IxDyn(&[2, 2]),
                vec![
                    Complex32::new(1.0, 0.0),
                    Complex32::new(0.0, 1.0),
                    Complex32::new(-1.0, 0.5),
                    Complex32::new(0.5, -0.25),
                ],
            )
            .unwrap(),
            &[0, 0],
        )
        .unwrap();
        rhs.put_slice(
            &ArrayD::from_shape_vec(
                IxDyn(&[2, 2]),
                vec![
                    Complex32::new(0.5, 0.5),
                    Complex32::new(1.0, 0.0),
                    Complex32::new(0.0, -0.5),
                    Complex32::new(-0.5, 0.25),
                ],
            )
            .unwrap(),
            &[0, 0],
        )
        .unwrap();

        let expr = ImageExpr::from_image(&lhs)
            .unwrap()
            .add_image(&rhs)
            .unwrap()
            .exp();
        let got = expr.get().unwrap();
        let expected = lhs.get().unwrap() + rhs.get().unwrap();
        for (lhs_value, rhs_value) in got.iter().zip(expected.iter()) {
            let diff = *lhs_value - rhs_value.exp();
            assert!(diff.norm() < 1.0e-5);
        }
    }

    #[test]
    fn float64_zip_wrapper_is_lazy_and_correct() {
        let mut lhs = make_persistent_image::<f64>(vec![2, 2]);
        let mut rhs = make_persistent_image::<f64>(vec![2, 2]);
        lhs.set(3.0).unwrap();
        rhs.set(4.0).unwrap();

        let expr = ImageExpr::zip(&lhs, &rhs, |a, b| a * b - 2.0).unwrap();
        assert_eq!(expr.get_at(&[1, 1]).unwrap(), 10.0);
    }

    #[test]
    fn complex64_map_wrapper_stays_lazy() {
        let mut image = make_persistent_image::<Complex64>(vec![2, 2]);
        image.set(Complex64::new(1.0, 2.0)).unwrap();

        let expr = ImageExpr::map(&image, |value| value * Complex64::new(2.0, 0.0)).unwrap();
        assert_eq!(expr.get_at(&[0, 0]).unwrap(), Complex64::new(2.0, 4.0));
    }

    #[test]
    fn comparison_and_logical_masks_are_lazy() {
        let mut image = make_persistent_image::<f32>(vec![2, 3]);
        image
            .put_slice(
                &ArrayD::from_shape_vec(IxDyn(&[2, 3]), vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0])
                    .unwrap(),
                &[0, 0],
            )
            .unwrap();

        let mask = ImageExpr::from_image(&image)
            .unwrap()
            .gt_scalar(1.0)
            .and(ImageExpr::from_image(&image).unwrap().lt_scalar(4.5))
            .unwrap()
            .logical_not();

        let got = mask.get().unwrap();
        let expected =
            ArrayD::from_shape_vec(IxDyn(&[2, 3]), vec![true, true, false, false, false, true])
                .unwrap();
        assert_eq!(got, expected);
    }

    #[test]
    fn save_as_materializes_the_current_expression() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("expr.image");
        let mut image = make_persistent_image::<f32>(vec![2, 2]);
        image.set(2.0).unwrap();

        let expr = ImageExpr::from_image(&image).unwrap().multiply_scalar(3.0);
        let compiled = expr.compile().unwrap();
        assert_eq!(compiled.get().unwrap().sum(), 24.0);
        let saved = compiled.save_as(&out).unwrap();

        assert_eq!(saved.get().unwrap().sum(), 24.0);
    }

    #[test]
    fn compiled_save_as_carries_propagated_default_mask() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("masked_expr.image");
        let mut image = crate::TempImage::<f32>::new(vec![2, 2], CoordinateSystem::new()).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0, 2.0, 3.0, 4.0]).unwrap(),
                &[0, 0],
            )
            .unwrap();
        image.make_mask("quality", true, true).unwrap();
        image
            .put_mask(
                "quality",
                &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![true, false, true, false]).unwrap(),
            )
            .unwrap();

        let expr = ImageExpr::from_image(&image).unwrap().multiply_scalar(2.0);
        let compiled = expr.compile().unwrap();
        assert_eq!(
            compiled.source_mask().unwrap(),
            ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![true, false, true, false]).unwrap()
        );
        let saved = compiled.save_as(&out).unwrap();

        assert_eq!(
            saved.get().unwrap(),
            ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![2.0, 4.0, 6.0, 8.0]).unwrap()
        );
        assert_eq!(saved.default_mask_name().as_deref(), Some("compiled_mask"));
        assert_eq!(
            saved.get_mask().unwrap().unwrap(),
            ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![true, false, true, false]).unwrap()
        );
    }

    #[test]
    fn read_only_mutation_paths_are_explicit() {
        let mut image = make_persistent_image::<f32>(vec![1, 1]);
        image.set(1.0).unwrap();
        let mut expr = ImageExpr::from_image(&image).unwrap().add_scalar(2.0);

        assert!(matches!(
            expr.set(0.0),
            Err(ImageError::ReadOnly("ImageExpr"))
        ));
        assert!(matches!(
            expr.put_at(0.0, &[0, 0]),
            Err(ImageError::ReadOnly("ImageExpr"))
        ));
    }

    // ---- Wave 11b: exhaustive operator matrix tests ----

    #[test]
    fn transcendental_unary_ops_f32() {
        let mut image = make_persistent_image::<f32>(vec![2, 2]);
        image
            .put_slice(
                &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![0.5, 1.0, 0.25, 0.75]).unwrap(),
                &[0, 0],
            )
            .unwrap();

        let e = |op| ImageExpr::from_image(&image).unwrap().unary(op);

        let check = |op: ImageExprUnaryOp, expected_fn: fn(f32) -> f32| {
            let got = e(op).get().unwrap();
            let src = image.get().unwrap();
            for (g, s) in got.iter().zip(src.iter()) {
                let want = expected_fn(*s);
                assert!(
                    (g - want).abs() < 1e-6,
                    "{op:?}: got {g}, want {want} for input {s}"
                );
            }
        };

        check(ImageExprUnaryOp::Sin, f32::sin);
        check(ImageExprUnaryOp::Cos, f32::cos);
        check(ImageExprUnaryOp::Tan, f32::tan);
        check(ImageExprUnaryOp::Asin, f32::asin);
        check(ImageExprUnaryOp::Acos, f32::acos);
        check(ImageExprUnaryOp::Atan, f32::atan);
        check(ImageExprUnaryOp::Sinh, f32::sinh);
        check(ImageExprUnaryOp::Cosh, f32::cosh);
        check(ImageExprUnaryOp::Tanh, f32::tanh);
        check(ImageExprUnaryOp::Exp, f32::exp);
        check(ImageExprUnaryOp::Log, f32::ln);
        check(ImageExprUnaryOp::Log10, f32::log10);
        check(ImageExprUnaryOp::Sqrt, f32::sqrt);
        check(ImageExprUnaryOp::Abs, f32::abs);
        check(ImageExprUnaryOp::Ceil, f32::ceil);
        check(ImageExprUnaryOp::Floor, f32::floor);
        check(ImageExprUnaryOp::Round, f32::round);
        check(ImageExprUnaryOp::Sign, |x| {
            if x > 0.0 {
                1.0
            } else if x < 0.0 {
                -1.0
            } else {
                0.0
            }
        });
        check(ImageExprUnaryOp::Conj, |x| x); // identity for real
    }

    #[test]
    fn transcendental_unary_ops_f64() {
        let mut image = make_persistent_image::<f64>(vec![2, 2]);
        image
            .put_slice(
                &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![0.5, 1.0, 0.25, 0.75]).unwrap(),
                &[0, 0],
            )
            .unwrap();

        let got_sqrt = ImageExpr::from_image(&image).unwrap().sqrt().get().unwrap();
        let src = image.get().unwrap();
        for (g, s) in got_sqrt.iter().zip(src.iter()) {
            assert!((g - s.sqrt()).abs() < 1e-14);
        }

        let got_log = ImageExpr::from_image(&image).unwrap().log().get().unwrap();
        for (g, s) in got_log.iter().zip(src.iter()) {
            assert!((g - s.ln()).abs() < 1e-14);
        }
    }

    #[test]
    fn transcendental_unary_ops_complex32() {
        let mut image = make_persistent_image::<Complex32>(vec![2, 2]);
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[2, 2]),
                    vec![
                        Complex32::new(1.0, 0.5),
                        Complex32::new(0.5, -0.25),
                        Complex32::new(-0.5, 1.0),
                        Complex32::new(0.25, 0.75),
                    ],
                )
                .unwrap(),
                &[0, 0],
            )
            .unwrap();

        let src = image.get().unwrap();

        // sin
        let got = ImageExpr::from_image(&image).unwrap().sin().get().unwrap();
        for (g, s) in got.iter().zip(src.iter()) {
            let want = s.sin();
            assert!((g - want).norm() < 1e-5, "sin: got {g}, want {want}");
        }

        // sqrt
        let got = ImageExpr::from_image(&image).unwrap().sqrt().get().unwrap();
        for (g, s) in got.iter().zip(src.iter()) {
            let want = s.sqrt();
            assert!((g - want).norm() < 1e-5, "sqrt: got {g}, want {want}");
        }

        // conj
        let got = ImageExpr::from_image(&image).unwrap().conj().get().unwrap();
        for (g, s) in got.iter().zip(src.iter()) {
            let want = s.conj();
            assert_eq!(*g, want);
        }

        // abs returns Complex(|z|, 0)
        let got = ImageExpr::from_image(&image).unwrap().abs().get().unwrap();
        for (g, s) in got.iter().zip(src.iter()) {
            assert!((g.re - s.norm()).abs() < 1e-5);
            assert_eq!(g.im, 0.0);
        }
    }

    #[test]
    fn binary_function_ops_f32() {
        let mut lhs = make_persistent_image::<f32>(vec![2, 2]);
        let mut rhs = make_persistent_image::<f32>(vec![2, 2]);
        lhs.put_slice(
            &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![4.0, 9.0, 16.0, 25.0]).unwrap(),
            &[0, 0],
        )
        .unwrap();
        rhs.put_slice(
            &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![0.5, 0.5, 0.5, 0.5]).unwrap(),
            &[0, 0],
        )
        .unwrap();

        // pow
        let got = ImageExpr::from_image(&lhs)
            .unwrap()
            .pow_scalar(0.5)
            .get()
            .unwrap();
        let expected = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![2.0, 3.0, 4.0, 5.0]).unwrap();
        for (g, e) in got.iter().zip(expected.iter()) {
            assert!((g - e).abs() < 1e-5);
        }

        // min
        let got = ImageExpr::from_image(&lhs)
            .unwrap()
            .min_scalar(10.0)
            .get()
            .unwrap();
        let expected = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![4.0, 9.0, 10.0, 10.0]).unwrap();
        assert_eq!(got, expected);

        // max
        let got = ImageExpr::from_image(&lhs)
            .unwrap()
            .max_scalar(10.0)
            .get()
            .unwrap();
        let expected =
            ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![10.0, 10.0, 16.0, 25.0]).unwrap();
        assert_eq!(got, expected);

        // fmod
        let got = ImageExpr::from_image(&lhs)
            .unwrap()
            .fmod_scalar(7.0)
            .get()
            .unwrap();
        let expected = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![4.0, 2.0, 2.0, 4.0]).unwrap();
        assert_eq!(got, expected);

        // atan2 (expr variant)
        let got = ImageExpr::from_image(&lhs)
            .unwrap()
            .atan2_expr(ImageExpr::from_image(&rhs).unwrap())
            .unwrap()
            .get()
            .unwrap();
        let lhs_data = lhs.get().unwrap();
        let rhs_data = rhs.get().unwrap();
        for ((g, l), r) in got.iter().zip(lhs_data.iter()).zip(rhs_data.iter()) {
            assert!((g - l.atan2(*r)).abs() < 1e-5);
        }
    }

    #[test]
    fn subtract_and_divide_convenience_methods() {
        let mut lhs = make_persistent_image::<f32>(vec![2, 2]);
        let mut rhs = make_persistent_image::<f32>(vec![2, 2]);
        lhs.put_slice(
            &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![10.0, 20.0, 30.0, 40.0]).unwrap(),
            &[0, 0],
        )
        .unwrap();
        rhs.put_slice(
            &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0, 2.0, 3.0, 4.0]).unwrap(),
            &[0, 0],
        )
        .unwrap();

        let sub = ImageExpr::from_image(&lhs)
            .unwrap()
            .subtract_image(&rhs)
            .unwrap()
            .get()
            .unwrap();
        let expected = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![9.0, 18.0, 27.0, 36.0]).unwrap();
        assert_eq!(sub, expected);

        let div = ImageExpr::from_image(&lhs)
            .unwrap()
            .divide_scalar(10.0)
            .get()
            .unwrap();
        let expected = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0, 2.0, 3.0, 4.0]).unwrap();
        assert_eq!(div, expected);
    }

    #[test]
    fn extended_comparison_ops() {
        let mut image = make_persistent_image::<f32>(vec![2, 3]);
        image
            .put_slice(
                &ArrayD::from_shape_vec(IxDyn(&[2, 3]), vec![1.0, 2.0, 3.0, 4.0, 5.0, 3.0])
                    .unwrap(),
                &[0, 0],
            )
            .unwrap();

        // ge
        let got = ImageExpr::from_image(&image)
            .unwrap()
            .ge_scalar(3.0)
            .get()
            .unwrap();
        let expected =
            ArrayD::from_shape_vec(IxDyn(&[2, 3]), vec![false, false, true, true, true, true])
                .unwrap();
        assert_eq!(got, expected);

        // le
        let got = ImageExpr::from_image(&image)
            .unwrap()
            .le_scalar(3.0)
            .get()
            .unwrap();
        let expected =
            ArrayD::from_shape_vec(IxDyn(&[2, 3]), vec![true, true, true, false, false, true])
                .unwrap();
        assert_eq!(got, expected);

        // eq
        let got = ImageExpr::from_image(&image)
            .unwrap()
            .eq_scalar(3.0)
            .get()
            .unwrap();
        let expected =
            ArrayD::from_shape_vec(IxDyn(&[2, 3]), vec![false, false, true, false, false, true])
                .unwrap();
        assert_eq!(got, expected);

        // ne
        let got = ImageExpr::from_image(&image)
            .unwrap()
            .ne_scalar(3.0)
            .get()
            .unwrap();
        let expected =
            ArrayD::from_shape_vec(IxDyn(&[2, 3]), vec![true, true, false, true, true, false])
                .unwrap();
        assert_eq!(got, expected);
    }

    #[test]
    fn chained_transcendental_stays_lazy() {
        let data = ArrayD::from_shape_fn(IxDyn(&[4, 4]), |idx| (idx[0] * 4 + idx[1]) as f32 * 0.1);
        let (image, slice_shapes) = CountingImage::new(data.clone());

        // sin(sqrt(x + 1.0))
        let expr = ImageExpr::from_image(&image)
            .unwrap()
            .add_scalar(1.0)
            .sqrt()
            .sin();
        let slice = expr.get_slice(&[1, 1], &[2, 2]).unwrap();

        assert_eq!(slice.shape(), &[2, 2]);
        let recorded = slice_shapes.borrow();
        assert!(recorded.iter().all(|shape| shape == &vec![2, 2]));

        // Verify values
        for i in 0..2 {
            for j in 0..2 {
                let src = data[IxDyn(&[i + 1, j + 1])];
                let want = (src + 1.0).sqrt().sin();
                assert!((slice[IxDyn(&[i, j])] - want).abs() < 1e-6);
            }
        }
    }

    #[test]
    fn edge_case_empty_and_degenerate() {
        // Degenerate 1-element image
        let mut image = make_persistent_image::<f32>(vec![1, 1]);
        image.set(4.0).unwrap();

        let got = ImageExpr::from_image(&image).unwrap().sqrt().get().unwrap();
        assert_eq!(got[IxDyn(&[0, 0])], 2.0);

        // Check that sign works on negative values
        let mut neg = make_persistent_image::<f32>(vec![1, 3]);
        neg.put_slice(
            &ArrayD::from_shape_vec(IxDyn(&[1, 3]), vec![-2.0, 0.0, 3.0]).unwrap(),
            &[0, 0],
        )
        .unwrap();
        let got = ImageExpr::from_image(&neg).unwrap().sign().get().unwrap();
        let expected = ArrayD::from_shape_vec(IxDyn(&[1, 3]), vec![-1.0, 0.0, 1.0]).unwrap();
        assert_eq!(got, expected);
    }

    #[test]
    fn min_max_expr_variant() {
        let mut a = make_persistent_image::<f32>(vec![2, 2]);
        let mut b = make_persistent_image::<f32>(vec![2, 2]);
        a.put_slice(
            &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0, 5.0, 3.0, 7.0]).unwrap(),
            &[0, 0],
        )
        .unwrap();
        b.put_slice(
            &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![4.0, 2.0, 6.0, 0.0]).unwrap(),
            &[0, 0],
        )
        .unwrap();

        let got = ImageExpr::from_image(&a)
            .unwrap()
            .min_expr(ImageExpr::from_image(&b).unwrap())
            .unwrap()
            .get()
            .unwrap();
        let expected = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0, 2.0, 3.0, 0.0]).unwrap();
        assert_eq!(got, expected);

        let got = ImageExpr::from_image(&a)
            .unwrap()
            .max_expr(ImageExpr::from_image(&b).unwrap())
            .unwrap()
            .get()
            .unwrap();
        let expected = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![4.0, 5.0, 6.0, 7.0]).unwrap();
        assert_eq!(got, expected);
    }

    #[test]
    fn binary_math_image_helpers_cover_remaining_families() {
        let mut lhs = make_persistent_image::<f32>(vec![2, 2]);
        let mut rhs = make_persistent_image::<f32>(vec![2, 2]);
        lhs.put_slice(
            &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![3.0, 4.5, 5.0, 7.5]).unwrap(),
            &[0, 0],
        )
        .unwrap();
        rhs.put_slice(
            &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![2.0, 2.0, 3.0, 2.5]).unwrap(),
            &[0, 0],
        )
        .unwrap();

        let pow = ImageExpr::from_image(&lhs)
            .unwrap()
            .pow_image(&rhs)
            .unwrap()
            .get()
            .unwrap();
        let fmod = ImageExpr::from_image(&lhs)
            .unwrap()
            .fmod_image(&rhs)
            .unwrap()
            .get()
            .unwrap();
        let atan2 = ImageExpr::from_image(&lhs)
            .unwrap()
            .atan2_image(&rhs)
            .unwrap()
            .get()
            .unwrap();

        let lhs_data = lhs.get().unwrap();
        let rhs_data = rhs.get().unwrap();
        for (((pow_got, fmod_got), atan2_got), (l, r)) in pow
            .iter()
            .zip(fmod.iter())
            .zip(atan2.iter())
            .zip(lhs_data.iter().zip(rhs_data.iter()))
        {
            assert!((*pow_got - l.powf(*r)).abs() < 1e-5);
            assert!((*fmod_got - (*l % *r)).abs() < 1e-5);
            assert!((*atan2_got - l.atan2(*r)).abs() < 1e-5);
        }
    }

    // ========================================================================
    // Wave 14a — ISNAN, NDIM, LENGTH, NELEM
    // ========================================================================

    #[test]
    fn isnan_f32() {
        let data =
            ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0f32, f32::NAN, 3.0, f32::NAN]).unwrap();
        let (image, _) = CountingImage::new(data);
        let mask = ImageExpr::from_image(&image).unwrap().isnan();
        let result = mask.get().unwrap();
        assert_eq!(
            result,
            ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![false, true, false, true]).unwrap()
        );
    }

    #[test]
    fn expr_isnan_complex32_trait() {
        // Complex32 lacks PartialOrd so MaskExpr evaluation isn't available.
        // Test the underlying trait method directly.
        assert!(!Complex32::new(1.0, 2.0).expr_isnan());
        assert!(Complex32::new(f32::NAN, 0.0).expr_isnan());
        assert!(Complex32::new(0.0, f32::NAN).expr_isnan());
        assert!(!Complex64::new(1.0, 2.0).expr_isnan());
        assert!(Complex64::new(f64::NAN, 0.0).expr_isnan());
    }

    #[test]
    fn ndim_length_nelem() {
        let data = ArrayD::from_elem(IxDyn(&[4, 5, 6]), 1.0f32);
        let (image, _) = CountingImage::new(data);
        let expr = ImageExpr::from_image(&image).unwrap();
        assert_eq!(expr.ndim_value(), 3);
        assert_eq!(expr.nelem_value(), 120);
        assert_eq!(expr.length_value(0), Some(4));
        assert_eq!(expr.length_value(1), Some(5));
        assert_eq!(expr.length_value(2), Some(6));
    }

    // ========================================================================
    // Wave 14b — Reductions
    // ========================================================================

    /// Extract the single scalar from a 0-D reduction result.
    fn scalar(expr: &ImageExpr<f32>) -> f32 {
        assert!(expr.shape().is_empty(), "expected 0-D reduction");
        expr.get().unwrap()[IxDyn(&[])]
    }

    #[test]
    fn sum_reduce() {
        let data = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0f32, 2.0, 3.0, 4.0]).unwrap();
        let (image, _) = CountingImage::new(data);
        let expr = ImageExpr::from_image(&image).unwrap().sum_reduce();
        assert!((scalar(&expr) - 10.0).abs() < 1e-5);
    }

    #[test]
    fn min_max_reduce() {
        let data = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![3.0f32, 1.0, 4.0, 2.0]).unwrap();
        let (image, _) = CountingImage::new(data);
        let min_expr = ImageExpr::from_image(&image).unwrap().min_reduce();
        assert!((scalar(&min_expr) - 1.0).abs() < 1e-5);
        let max_expr = ImageExpr::from_image(&image).unwrap().max_reduce();
        assert!((scalar(&max_expr) - 4.0).abs() < 1e-5);
    }

    #[test]
    fn mean_reduce() {
        let data = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0f32, 2.0, 3.0, 4.0]).unwrap();
        let (image, _) = CountingImage::new(data);
        let expr = ImageExpr::from_image(&image).unwrap().mean_reduce();
        assert!((scalar(&expr) - 2.5).abs() < 1e-5);
    }

    #[test]
    fn median_reduce() {
        let data = ArrayD::from_shape_vec(IxDyn(&[5]), vec![5.0f32, 1.0, 3.0, 4.0, 2.0]).unwrap();
        let (image, _) = CountingImage::new(data);
        let expr = ImageExpr::from_image(&image).unwrap().median_reduce();
        assert!((scalar(&expr) - 3.0).abs() < 1e-5);
    }

    #[test]
    fn fractile_is_median_at_half() {
        let data = ArrayD::from_shape_vec(IxDyn(&[5]), vec![5.0f32, 1.0, 3.0, 4.0, 2.0]).unwrap();
        let (image, _) = CountingImage::new(data);
        let expr = ImageExpr::from_image(&image).unwrap().fractile(0.5);
        assert!((scalar(&expr) - 3.0).abs() < 1e-5);
    }

    #[test]
    fn fractile_range() {
        let data =
            ArrayD::from_shape_vec(IxDyn(&[5]), vec![10.0f32, 20.0, 30.0, 40.0, 50.0]).unwrap();
        let (image, _) = CountingImage::new(data);
        let expr = ImageExpr::from_image(&image)
            .unwrap()
            .fractile_range(0.25, 0.75);
        // fractile(0.25) = data[1] = 20, fractile(0.75) = data[3] = 40, range = 20
        assert!((scalar(&expr) - 20.0).abs() < 1e-5);
    }

    #[test]
    fn single_element_reduction() {
        let data = ArrayD::from_shape_vec(IxDyn(&[1]), vec![42.0f32]).unwrap();
        let (image, _) = CountingImage::new(data);
        let check = |expr: ImageExpr<f32>| assert!((scalar(&expr) - 42.0).abs() < 1e-5);
        check(ImageExpr::from_image(&image).unwrap().sum_reduce());
        check(ImageExpr::from_image(&image).unwrap().min_reduce());
        check(ImageExpr::from_image(&image).unwrap().max_reduce());
        check(ImageExpr::from_image(&image).unwrap().mean_reduce());
        check(ImageExpr::from_image(&image).unwrap().median_reduce());
    }

    #[test]
    fn reduction_broadcasts_in_binary() {
        // sum(a) + a should broadcast scalar to [2,2]
        let data = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0f32, 2.0, 3.0, 4.0]).unwrap();
        let (image, _) = CountingImage::new(data);
        let sum_expr = ImageExpr::from_image(&image).unwrap().sum_reduce();
        let img_expr = ImageExpr::from_image(&image).unwrap();
        let combined = sum_expr.add_expr(img_expr).unwrap();
        assert_eq!(combined.shape(), &[2, 2]);
        let result = combined.get().unwrap();
        // sum = 10, so result = [11, 12, 13, 14]
        let expected =
            ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![11.0f32, 12.0, 13.0, 14.0]).unwrap();
        assert_eq!(result, expected);
    }

    // ========================================================================
    // Wave 14c — Conditional, mask interaction, boolean reductions
    // ========================================================================

    #[test]
    fn iif_conditional() {
        let data = ArrayD::from_shape_vec(IxDyn(&[4]), vec![1.0f32, 5.0, 3.0, 7.0]).unwrap();
        let (image, _) = CountingImage::new(data);
        let mask = ImageExpr::from_image(&image)
            .unwrap()
            .compare_scalar(4.0, ImageExprCompareOp::GreaterThan);
        let t_val = ImageExpr::from_image(&image).unwrap().add_scalar(100.0);
        let f_val = ImageExpr::from_image(&image).unwrap().add_scalar(0.0);
        let result = ImageExpr::iif(mask, t_val, f_val).unwrap().get().unwrap();
        // [1>4?=F, 5>4?=T, 3>4?=F, 7>4?=T] → [1, 105, 3, 107]
        let expected =
            ArrayD::from_shape_vec(IxDyn(&[4]), vec![1.0f32, 105.0, 3.0, 107.0]).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn all_any_reduce() {
        let data = ArrayD::from_shape_vec(IxDyn(&[4]), vec![1.0f32, 2.0, 3.0, 4.0]).unwrap();
        let (image, _) = CountingImage::new(data);

        let mask_scalar = |m: MaskExpr<f32>| -> bool {
            assert!(m.shape().is_empty(), "expected 0-D mask");
            m.get().unwrap()[IxDyn(&[])]
        };

        let all_gt_0 = ImageExpr::from_image(&image)
            .unwrap()
            .compare_scalar(0.0, ImageExprCompareOp::GreaterThan)
            .all_reduce();
        assert!(mask_scalar(all_gt_0));

        let all_gt_2 = ImageExpr::from_image(&image)
            .unwrap()
            .compare_scalar(2.0, ImageExprCompareOp::GreaterThan)
            .all_reduce();
        assert!(!mask_scalar(all_gt_2));

        let any_gt_3 = ImageExpr::from_image(&image)
            .unwrap()
            .compare_scalar(3.0, ImageExprCompareOp::GreaterThan)
            .any_reduce();
        assert!(mask_scalar(any_gt_3));

        let any_gt_10 = ImageExpr::from_image(&image)
            .unwrap()
            .compare_scalar(10.0, ImageExprCompareOp::GreaterThan)
            .any_reduce();
        assert!(!mask_scalar(any_gt_10));
    }

    #[test]
    fn ntrue_nfalse() {
        let data = ArrayD::from_shape_vec(IxDyn(&[4]), vec![1.0f32, 5.0, 3.0, 7.0]).unwrap();
        let (image, _) = CountingImage::new(data);
        let mask_t = ImageExpr::from_image(&image)
            .unwrap()
            .compare_scalar(4.0, ImageExprCompareOp::GreaterThan);
        let mask_f = ImageExpr::from_image(&image)
            .unwrap()
            .compare_scalar(4.0, ImageExprCompareOp::GreaterThan);
        let ntrue_expr = ImageExpr::<f32>::ntrue(mask_t);
        let nfalse_expr = ImageExpr::<f32>::nfalse(mask_f);
        assert!((scalar(&ntrue_expr) - 2.0).abs() < 1e-5);
        assert!((scalar(&nfalse_expr) - 2.0).abs() < 1e-5);
    }

    #[test]
    fn constant_mask() {
        let mask_data =
            ArrayD::from_shape_vec(IxDyn(&[4]), vec![true, false, true, false]).unwrap();
        let mask = MaskExpr::<f32>::from_constant(mask_data.clone());
        let result = mask.get().unwrap();
        assert_eq!(result, mask_data);
    }

    #[test]
    fn replace_masked_pixels() {
        let primary_data =
            ArrayD::from_shape_vec(IxDyn(&[4]), vec![1.0f32, 2.0, 3.0, 4.0]).unwrap();
        let (primary_img, _) = CountingImage::new(primary_data);
        let replacement_data = ArrayD::from_elem(IxDyn(&[4]), 0.0f32);
        let (replacement_img, _) = CountingImage::new(replacement_data);
        let mask = ArrayD::from_shape_vec(IxDyn(&[4]), vec![true, false, true, false]).unwrap();
        let result = ImageExpr::from_image(&primary_img)
            .unwrap()
            .replace(ImageExpr::from_image(&replacement_img).unwrap(), mask)
            .unwrap()
            .get()
            .unwrap();
        // mask=true keeps primary, mask=false uses replacement
        let expected = ArrayD::from_shape_vec(IxDyn(&[4]), vec![1.0f32, 0.0, 3.0, 0.0]).unwrap();
        assert_eq!(result, expected);
    }

    // ========================================================================
    // Wave 14d — Type-changing functions
    // ========================================================================

    #[test]
    fn complex32_real_part() {
        let data = ArrayD::from_shape_vec(
            IxDyn(&[2]),
            vec![Complex32::new(3.0, 4.0), Complex32::new(1.0, 2.0)],
        )
        .unwrap();
        let (image, _) = CountingImage::new(data);
        let result = ImageExpr::from_image(&image)
            .unwrap()
            .real_part()
            .get()
            .unwrap();
        let expected = ArrayD::from_shape_vec(IxDyn(&[2]), vec![3.0f32, 1.0]).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn complex32_imag_part() {
        let data = ArrayD::from_shape_vec(
            IxDyn(&[2]),
            vec![Complex32::new(3.0, 4.0), Complex32::new(1.0, 2.0)],
        )
        .unwrap();
        let (image, _) = CountingImage::new(data);
        let result = ImageExpr::from_image(&image)
            .unwrap()
            .imag_part()
            .get()
            .unwrap();
        let expected = ArrayD::from_shape_vec(IxDyn(&[2]), vec![4.0f32, 2.0]).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn complex32_arg_phase() {
        let data = ArrayD::from_shape_vec(
            IxDyn(&[2]),
            vec![Complex32::new(1.0, 1.0), Complex32::new(0.0, 1.0)],
        )
        .unwrap();
        let (image, _) = CountingImage::new(data);
        let result = ImageExpr::from_image(&image)
            .unwrap()
            .arg_phase()
            .get()
            .unwrap();
        assert!((result[[0]] - std::f32::consts::FRAC_PI_4).abs() < 1e-5);
        assert!((result[[1]] - std::f32::consts::FRAC_PI_2).abs() < 1e-5);
    }

    #[test]
    fn complex64_real_imag_arg() {
        let data = ArrayD::from_shape_vec(IxDyn(&[1]), vec![Complex64::new(3.0, 4.0)]).unwrap();
        let (image, _) = CountingImage::new(data);
        let re = ImageExpr::from_image(&image)
            .unwrap()
            .real_part()
            .get_at(&[0])
            .unwrap();
        let im = ImageExpr::from_image(&image)
            .unwrap()
            .imag_part()
            .get_at(&[0])
            .unwrap();
        let arg = ImageExpr::from_image(&image)
            .unwrap()
            .arg_phase()
            .get_at(&[0])
            .unwrap();
        assert!((re - 3.0).abs() < 1e-10);
        assert!((im - 4.0).abs() < 1e-10);
        assert!((arg - 4.0f64.atan2(3.0)).abs() < 1e-10);
    }

    #[test]
    fn f32_to_complex() {
        let re_data = ArrayD::from_shape_vec(IxDyn(&[2]), vec![1.0f32, 3.0]).unwrap();
        let im_data = ArrayD::from_shape_vec(IxDyn(&[2]), vec![2.0f32, 4.0]).unwrap();
        let (re_img, _) = CountingImage::new(re_data);
        let (im_img, _) = CountingImage::new(im_data);
        let result = ImageExpr::from_image(&re_img)
            .unwrap()
            .to_complex(ImageExpr::from_image(&im_img).unwrap())
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(result[[0]], Complex32::new(1.0, 2.0));
        assert_eq!(result[[1]], Complex32::new(3.0, 4.0));
    }

    #[test]
    fn f64_to_complex() {
        let re_data = ArrayD::from_shape_vec(IxDyn(&[2]), vec![1.0f64, 3.0]).unwrap();
        let im_data = ArrayD::from_shape_vec(IxDyn(&[2]), vec![2.0f64, 4.0]).unwrap();
        let (re_img, _) = CountingImage::new(re_data);
        let (im_img, _) = CountingImage::new(im_data);
        let result = ImageExpr::from_image(&re_img)
            .unwrap()
            .to_complex(ImageExpr::from_image(&im_img).unwrap())
            .unwrap()
            .get()
            .unwrap();
        assert_eq!(result[[0]], Complex64::new(1.0, 2.0));
        assert_eq!(result[[1]], Complex64::new(3.0, 4.0));
    }
}
