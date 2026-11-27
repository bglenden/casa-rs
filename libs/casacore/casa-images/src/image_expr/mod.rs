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
//!   `!=`, AND, OR, NOT via [`MaskExprBuilder`]
//! - **NaN test**: `isnan(expr)` via [`MaskExprBuilder`]
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
//!   calling [`ImageExprBuilder::real_part`], [`ImageExprBuilder::imag_part`],
//!   [`ImageExprBuilder::arg_phase`], or [`ImageExprBuilder::to_complex`] directly.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

use casa_coordinates::CoordinateSystem;
use casa_core::{ArrayD, RecordValue, Value};
use casa_lattices::LatticeError;
use ndarray::Zip;

use crate::error::ImageError;
use crate::image::{ImageInterface, ImagePixel, PagedImage};
use crate::image_expr_lel::{
    broadcast_shapes, can_broadcast_to, choose_rhs_meta, combine_optional_masks, rebin_coordinates,
    rebin_mask_slice, rebin_shape, replace_stokes_with_pangle, spectral_frequencies_hz,
};
use crate::image_info::ImageInfo;

mod compiled;
pub use compiled::MemorySnapshotImage;
pub use compiled::bounded_read_cursor;
pub use compiled::{ImageExpression, MaskExpression};

mod builder;

type UnaryExprFn<T> = Arc<dyn Fn(T) -> T + Send + Sync>;
type BinaryExprFn<T> = Arc<dyn Fn(T, T) -> T + Send + Sync>;

/// Numeric operations supported by the lazy image expression DAG.
///
/// Extends [`ImagePixel`] with mathematical functions dispatched by
/// [`ImageExprUnaryOp`] and [`ImageExprBinaryOp`].  Implemented for
/// [`f32`], [`f64`], [`Complex32`](casa_core::Complex32), and
/// [`Complex64`](casa_core::Complex64).
pub trait ImageExprValue:
    ImagePixel
    + std::ops::Add<Output = Self>
    + std::ops::Sub<Output = Self>
    + std::ops::Mul<Output = Self>
    + std::ops::Div<Output = Self>
    + std::ops::Neg<Output = Self>
{
    // Transcendental / elementary functions
    /// Raises e to the pixel value.
    fn expr_exp(self) -> Self;
    /// Returns the sine of the input, with real arguments in radians.
    fn expr_sin(self) -> Self;
    /// Returns the cosine of the input, with real arguments in radians.
    fn expr_cos(self) -> Self;
    /// Returns the tangent of the input, with real arguments in radians.
    fn expr_tan(self) -> Self;
    /// Returns the principal arcsine, in radians for real values.
    fn expr_asin(self) -> Self;
    /// Returns the principal arccosine, in radians for real values.
    fn expr_acos(self) -> Self;
    /// Returns the principal arctangent, in radians for real values.
    fn expr_atan(self) -> Self;
    /// Returns the hyperbolic sine of the input.
    fn expr_sinh(self) -> Self;
    /// Returns the hyperbolic cosine of the input.
    fn expr_cosh(self) -> Self;
    /// Returns the hyperbolic tangent of the input.
    fn expr_tanh(self) -> Self;
    /// Returns the natural logarithm.
    fn expr_log(self) -> Self;
    /// Returns the base-ten logarithm.
    fn expr_log10(self) -> Self;
    /// Returns the principal square root.
    fn expr_sqrt(self) -> Self;
    /// Returns the absolute value, or magnitude for a complex value.
    fn expr_abs(self) -> Self;
    /// Returns the complex conjugate, equal to the input for real values.
    fn expr_conj(self) -> Self;

    // Rounding / sign (component-wise for complex)
    /// Rounds each component toward positive infinity.
    fn expr_ceil(self) -> Self;
    /// Rounds each component toward negative infinity.
    fn expr_floor(self) -> Self;
    /// Rounds each component to the nearest integer, with ties away from zero.
    fn expr_round(self) -> Self;
    /// Returns the sign for a real input or the unit phasor for a nonzero complex input.
    fn expr_sign(self) -> Self;

    // Binary math functions
    /// Raises the left operand to the power of the right operand.
    fn expr_pow(self, rhs: Self) -> Self;
    /// Computes the floating remainder componentwise using a quotient truncated toward zero.
    fn expr_fmod(self, rhs: Self) -> Self;
    /// Computes the quadrant-aware angle in radians (x = rhs, y = self), componentwise for complex operands.
    fn expr_atan2(self, rhs: Self) -> Self;
    /// Returns the smaller real value or the complex operand with the smaller magnitude.
    fn expr_min(self, rhs: Self) -> Self;
    /// Returns the larger real value or the complex operand with the larger magnitude.
    fn expr_max(self, rhs: Self) -> Self;

    /// Returns `true` if this value is NaN.
    ///
    /// For complex types, returns `true` if either component is NaN.
    fn expr_isnan(&self) -> bool;

    /// Lossily convert from `f64`.  Used by reduction/scalar operations.
    fn from_f64_lossy(v: f64) -> Self;

    /// Divide by a real scalar in the pixel's native precision.
    fn div_by_real(self, divisor: f64) -> Self {
        self / Self::from_f64_lossy(divisor)
    }

    /// Lossily convert to `f64`.  Used by sorting in median/fractile.
    fn to_f64_lossy(&self) -> f64;

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

impl ImageExprValue for casa_core::Complex32 {
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
    fn div_by_real(self, divisor: f64) -> Self {
        self / divisor as f32
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

impl ImageExprValue for casa_core::Complex64 {
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
    fn div_by_real(self, divisor: f64) -> Self {
        self / divisor
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
/// `SUM`, `MIN1D`, `MAX1D`, `MEAN1D`, `MEDIAN1D`, `VARIANCE1D`, `STDDEV1D`, `AVDEV1D`.
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
    /// Sample variance (`n - 1` denominator) of valid elements.
    Variance,
    /// Sample standard deviation (`sqrt(variance)`) of valid elements.
    Stddev,
    /// Mean absolute deviation (`mean(|x - mean|)`) of valid elements.
    Avdev,
}

#[derive(Clone)]
/// Owned shape, WCS, brightness units, and auxiliary metadata for a lazy image expression source.
pub struct ImageExprMeta {
    shape: Vec<usize>,
    coords: CoordinateSystem,
    units: String,
    misc_info: casa_core::RecordValue,
    image_info: ImageInfo,
    name: Option<PathBuf>,
    legacy_coords: Option<RecordValue>,
    raw_image_info: Option<RecordValue>,
}

impl ImageExprMeta {
    /// Copies image geometry and metadata without reading pixels. Preserves raw CASA coordinate and
    /// beam records when the source exposes them.
    pub fn from_image<T: ImageExprValue>(
        image: &dyn ImageInterface<T>,
    ) -> Result<Self, ImageError> {
        let legacy_coords = image.as_any().and_then(|any| {
            if let Some(paged) = any.downcast_ref::<PagedImage<T>>() {
                paged
                    .table()
                    .keywords()
                    .get("coords")
                    .cloned()
                    .and_then(|v| match v {
                        Value::Record(r) => Some(r),
                        _ => None,
                    })
            } else if let Some(mem) = any.downcast_ref::<MemorySnapshotImage<T>>() {
                mem.legacy_coords.clone()
            } else if let Some(expression) = any.downcast_ref::<ImageExpression<T>>() {
                expression.legacy_coords_ref().cloned()
            } else {
                None
            }
        });
        let raw_image_info = image.as_any().and_then(|any| {
            if let Some(paged) = any.downcast_ref::<PagedImage<T>>() {
                match paged.table().keywords().get("imageinfo") {
                    Some(Value::Record(record)) => Some(record.clone()),
                    _ => None,
                }
            } else if let Some(expression) = any.downcast_ref::<ImageExpression<T>>() {
                expression.raw_image_info_ref().cloned()
            } else if let Some(temp) = any.downcast_ref::<crate::TempImage<T>>() {
                temp.image_info_record().cloned()
            } else {
                None
            }
        });
        Ok(Self {
            shape: image.shape().to_vec(),
            coords: image.coordinates().clone(),
            units: image.units().to_string(),
            misc_info: image.misc_info(),
            image_info: image.image_info()?,
            name: image.name().map(Path::to_path_buf),
            legacy_coords,
            raw_image_info,
        })
    }

    /// Copies geometry and units from a FITS import and decodes image and beam metadata. The path is
    /// retained as the expression source name.
    pub fn from_fits(
        fits: &crate::fits_read::FitsToolImport,
        path: &Path,
    ) -> Result<Self, ImageError> {
        let mut misc_info = fits.misc_info.clone();
        misc_info.remove("object");
        Ok(Self {
            shape: fits.shape().to_vec(),
            coords: fits.coordinates().clone(),
            units: fits.units.clone(),
            misc_info,
            image_info: ImageInfo::from_record(&fits.image_info)?,
            name: Some(path.to_path_buf()),
            legacy_coords: Some(fits.coordinate_record.clone()),
            raw_image_info: Some(fits.image_info.clone()),
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

type OwnedSliceReader<T> =
    Arc<dyn Fn(&[usize], &[usize], &[usize]) -> Result<ArrayD<T>, ImageError> + Send + Sync>;
/// Shared thread-safe validity reader. The arguments are the zero-based start, extent, and positive
/// stride of each axis. True pixels are valid.
pub type OwnedMaskReader = Arc<
    dyn Fn(&[usize], &[usize], &[usize]) -> Result<Option<ArrayD<bool>>, ImageError> + Send + Sync,
>;

#[derive(Clone)]
struct SectionSource<T> {
    shape: Vec<usize>,
    read: OwnedSliceReader<T>,
    mask: Option<OwnedMaskReader>,
}

#[derive(Clone)]
#[allow(clippy::arc_with_non_send_sync)]
enum NumericExprNode<'a, T: ImageExprValue> {
    Source(&'a dyn ImageInterface<T>),
    Sections(SectionSource<T>),
    Vector(Arc<ArrayD<T>>),
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
        empty_value: Option<T>,
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
        mask: Option<ArrayD<bool>>,
    },
    /// Type bridge: an opaque evaluation closure for cross-type operations.
    TypeBridge {
        eval_fn: SliceEvalFn<'a, T>,
    },
    /// Strip mask: `value(expr)` keeps values and clears pixel masks.
    StripMask {
        child: Box<NumericExprNode<'a, T>>,
    },
    /// Spectral index: `ln(lhs / rhs) / ln(f1 / f2)`.
    SpectralIndex {
        lhs: Box<NumericExprNode<'a, T>>,
        rhs: Box<NumericExprNode<'a, T>>,
        spec_axis_a: usize,
        freqs_a: Vec<f64>,
        spec_axis_b: usize,
        freqs_b: Vec<f64>,
    },
    /// Linear polarization angle in degrees: `0.5 * atan2(u, q) * 180 / pi`.
    Pa {
        u: Box<NumericExprNode<'a, T>>,
        q: Box<NumericExprNode<'a, T>>,
    },
    /// Block-averaged rebinning along leading or all axes.
    Rebin {
        child: Box<NumericExprNode<'a, T>>,
        in_shape: Vec<usize>,
        factors: Vec<usize>,
        out_shape: Vec<usize>,
    },
}

impl<'a, T: ImageExprValue> NumericExprNode<'a, T> {
    fn contains_conditional(&self) -> bool {
        match self {
            Self::Source(_)
            | Self::Sections(_)
            | Self::Vector(_)
            | Self::Scalar(_)
            | Self::StripMask { .. }
            | Self::TypeBridge { .. } => false,
            Self::Conditional { .. } => true,
            Self::UnaryOp { child, .. }
            | Self::CustomUnary { child, .. }
            | Self::Reduction { child, .. }
            | Self::Fractile { child, .. }
            | Self::FractileRange { child, .. }
            | Self::Rebin { child, .. } => child.contains_conditional(),
            Self::BinaryOp { lhs, rhs, .. }
            | Self::CustomBinary { lhs, rhs, .. }
            | Self::SpectralIndex { lhs, rhs, .. }
            | Self::Pa { u: lhs, q: rhs } => {
                lhs.contains_conditional() || rhs.contains_conditional()
            }
            Self::Replace { primary, .. } => primary.contains_conditional(),
            Self::MaskCount { mask, .. } => mask.contains_conditional(),
        }
    }

    fn try_shape(&self) -> Option<Vec<usize>> {
        match self {
            Self::Source(image) => Some(image.shape().to_vec()),
            Self::Sections(source) => Some(source.shape.clone()),
            Self::Vector(values) => Some(values.shape().to_vec()),
            Self::Scalar(_) => Some(vec![]),
            Self::UnaryOp { child, .. }
            | Self::CustomUnary { child, .. }
            | Self::StripMask { child } => child.try_shape(),
            Self::BinaryOp { lhs, rhs, .. }
            | Self::CustomBinary { lhs, rhs, .. }
            | Self::SpectralIndex { lhs, rhs, .. }
            | Self::Pa { u: lhs, q: rhs } => {
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
            Self::Rebin { out_shape, .. } => Some(out_shape.clone()),
            Self::TypeBridge { .. } => None,
        }
    }

    fn propagated_mask(&self) -> Result<Option<ArrayD<bool>>, ImageError> {
        match self {
            Self::Source(image) => image.default_mask(),
            Self::Sections(source) => match &source.mask {
                Some(mask) => {
                    let ndim = source.shape.len();
                    mask(&vec![0; ndim], &source.shape, &vec![1; ndim])
                }
                None => Ok(None),
            },
            Self::Scalar(_) | Self::Vector(_) | Self::StripMask { .. } => Ok(None),
            Self::UnaryOp { child, .. } | Self::CustomUnary { child, .. } => {
                child.propagated_mask()
            }
            Self::BinaryOp { lhs, rhs, .. }
            | Self::CustomBinary { lhs, rhs, .. }
            | Self::SpectralIndex { lhs, rhs, .. }
            | Self::Pa { u: lhs, q: rhs } => {
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
            Self::Replace { primary, .. } => primary.propagated_mask(),
            Self::Rebin {
                child,
                factors,
                out_shape,
                ..
            } => {
                let Some(mask) = child.propagated_mask()? else {
                    return Ok(None);
                };
                let stride = vec![1; out_shape.len()];
                Ok(Some(rebin_mask_slice(&mask, factors, out_shape, &stride)))
            }
            Self::Conditional { .. }
            | Self::Reduction { .. }
            | Self::Fractile { .. }
            | Self::FractileRange { .. }
            | Self::MaskCount { .. }
            | Self::TypeBridge { .. } => Ok(None),
        }
    }

    fn kind_name(&self) -> &'static str {
        match self {
            Self::Source(_) | Self::Sections(_) | Self::Vector(_) => "source",
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
            Self::StripMask { .. } => "value",
            Self::SpectralIndex { .. } => "spectralindex",
            Self::Pa { .. } => "pa",
            Self::Rebin { .. } => "rebin",
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
    /// Element-wise comparison of two numeric expressions, as LEL's
    /// image-to-image comparisons.
    CompareExpr {
        op: ImageExprCompareOp,
        lhs: Box<NumericExprNode<'a, T>>,
        rhs: Box<NumericExprNode<'a, T>>,
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
    /// Exposes the default pixel validity as Boolean values: `mask(expr)`.
    Validity {
        child: Box<NumericExprNode<'a, T>>,
    },
    BooleanValidity {
        child: Box<MaskExprNode<'a, T>>,
    },
    /// A constant boolean mask.
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
    /// Axis coordinate index membership test: `indexin(axis, [i0, ...])`.
    IndexIn {
        axis: i64,
        indices: Vec<i64>,
    },
}

impl<'a, T: ImageExprValue> MaskExprNode<'a, T> {
    fn contains_conditional(&self) -> bool {
        match self {
            Self::CompareScalar { expr, .. }
            | Self::IsNan { child: expr }
            | Self::Validity { child: expr } => expr.contains_conditional(),
            Self::CompareExpr { lhs, rhs, .. } => {
                lhs.contains_conditional() || rhs.contains_conditional()
            }
            Self::Logical { lhs, rhs, .. } => {
                lhs.contains_conditional() || rhs.contains_conditional()
            }
            Self::Not { child }
            | Self::BooleanValidity { child }
            | Self::AllReduce { child, .. }
            | Self::AnyReduce { child, .. } => child.contains_conditional(),
            Self::ConstantMask { .. } | Self::IndexIn { .. } => false,
        }
    }

    fn try_shape(&self) -> Option<Vec<usize>> {
        match self {
            Self::CompareScalar { expr, .. }
            | Self::IsNan { child: expr }
            | Self::Validity { child: expr } => expr.try_shape(),
            Self::CompareExpr { lhs, rhs, .. } => {
                let lhs_shape = lhs.try_shape()?;
                let rhs_shape = rhs.try_shape()?;
                broadcast_shapes(&lhs_shape, &rhs_shape).ok()
            }
            Self::Logical { lhs, rhs, .. } => {
                let lhs_shape = lhs.try_shape()?;
                let rhs_shape = rhs.try_shape()?;
                broadcast_shapes(&lhs_shape, &rhs_shape).ok()
            }
            Self::Not { child } | Self::BooleanValidity { child } => child.try_shape(),
            Self::ConstantMask { mask } => Some(mask.shape().to_vec()),
            Self::AllReduce { .. } | Self::AnyReduce { .. } | Self::IndexIn { .. } => Some(vec![]),
        }
    }

    #[cfg(test)]
    fn propagated_mask(&self) -> Result<Option<ArrayD<bool>>, ImageError> {
        match self {
            Self::CompareScalar { expr, .. } | Self::IsNan { child: expr } => {
                expr.propagated_mask()
            }
            Self::CompareExpr { lhs, rhs, .. } => {
                let (Some(ls), Some(rs)) = (lhs.try_shape(), rhs.try_shape()) else {
                    return Ok(None);
                };
                combine_optional_masks(lhs.propagated_mask()?, &ls, rhs.propagated_mask()?, &rs)
            }
            Self::Logical { lhs, rhs, .. } => {
                let (Some(ls), Some(rs)) = (lhs.try_shape(), rhs.try_shape()) else {
                    return Ok(None);
                };
                combine_optional_masks(lhs.propagated_mask()?, &ls, rhs.propagated_mask()?, &rs)
            }
            Self::Not { child } => child.propagated_mask(),
            Self::ConstantMask { .. }
            | Self::Validity { .. }
            | Self::BooleanValidity { .. }
            | Self::AllReduce { .. }
            | Self::AnyReduce { .. }
            | Self::IndexIn { .. } => Ok(None),
        }
    }
}

/// Checks that a zero-based strided request has the same rank as the lattice and every nonempty
/// extent lies within it. Zero strides and overflowing endpoints return an error.
pub fn validate_slice_request(
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

fn work_balanced_cursor_shape(
    shape: &[usize],
    worker_limit: usize,
) -> Result<Vec<usize>, ImageError> {
    if shape.is_empty() {
        return Ok(vec![]);
    }
    let total_pixels = shape
        .iter()
        .try_fold(1usize, |product, &extent| product.checked_mul(extent))
        .ok_or_else(|| ImageError::Lattice("expression shape product overflow".into()))?;
    let target_pixels = total_pixels.div_ceil(worker_limit.max(1)).max(1);
    let mut cursor = vec![1usize; shape.len()];
    let mut product = 1usize;
    for (axis, &extent) in shape.iter().enumerate() {
        let can_fit = target_pixels / product;
        if can_fit == 0 {
            break;
        }
        cursor[axis] = extent.min(can_fit);
        product = product
            .checked_mul(cursor[axis])
            .ok_or_else(|| ImageError::Lattice("expression cursor product overflow".into()))?;
    }
    Ok(cursor)
}

fn clamp_cursor_shape(cursor_shape: &[usize], full_shape: &[usize]) -> Vec<usize> {
    if cursor_shape.len() != full_shape.len() {
        return vec![1; full_shape.len()];
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
pub struct ImageExprBuilder<'a, T: ImageExprValue> {
    node: NumericExprNode<'a, T>,
    meta: ImageExprMeta,
    expr_string: Option<String>,
}

/// Lazy boolean mask expression built from numeric image expressions.
pub struct MaskExprBuilder<'a, T: ImageExprValue> {
    node: MaskExprNode<'a, T>,
    shape: Vec<usize>,
    metadata: Option<Box<ImageExprMeta>>,
}

impl<'a, T: ImageExprValue> Clone for MaskExprBuilder<'a, T> {
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            shape: self.shape.clone(),
            metadata: self.metadata.clone(),
        }
    }
}

impl<'a, T: ImageExprValue> fmt::Debug for MaskExprBuilder<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaskExprBuilder")
            .field("shape", &self.shape)
            .finish()
    }
}

impl<'a, T: ImageExprValue + PartialOrd> MaskExprBuilder<'a, T> {
    pub(crate) fn validity(self) -> Self {
        Self {
            shape: self.shape,
            metadata: self.metadata,
            node: MaskExprNode::BooleanValidity {
                child: Box::new(self.node),
            },
        }
    }

    /// Returns the shape that the canonical mask expression will produce.
    pub fn shape(&self) -> &[usize] {
        &self.shape
    }

    /// Borrows the mask's source WCS when its pixel-axis rank matches the resulting lattice.
    pub fn expression_coordinates(&self) -> Option<&CoordinateSystem> {
        self.metadata
            .as_ref()
            .filter(|meta| meta.coords.n_pixel_axes() == self.shape.len())
            .map(|meta| &meta.coords)
    }

    /// Combines two same-shaped masks with a logical operator.
    pub fn logical(self, rhs: Self, op: MaskLogicalOp) -> Result<Self, ImageError> {
        let out_shape = broadcast_shapes(&self.shape, &rhs.shape)?;
        let metadata = if choose_rhs_meta(&self.shape, &rhs.shape, &out_shape) {
            rhs.metadata.or(self.metadata)
        } else {
            self.metadata.or(rhs.metadata)
        };
        Ok(Self {
            node: MaskExprNode::Logical {
                op,
                lhs: Box::new(self.node),
                rhs: Box::new(rhs.node),
            },
            shape: out_shape,
            metadata,
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
            metadata: self.metadata,
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
            metadata: None,
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
            metadata: None,
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
            metadata: None,
        }
    }

    /// Creates a shape-polymorphic `indexin(axis, indices)` mask expression.
    ///
    /// Evaluates to `true` where the 0-based coordinate on `axis` appears in `indices`.
    pub fn index_in(axis: i64, indices: Vec<i64>) -> Self {
        Self {
            node: MaskExprNode::IndexIn { axis, indices },
            shape: vec![],
            metadata: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn source_mask(&self) -> Result<Option<ArrayD<bool>>, ImageError> {
        self.node.propagated_mask()
    }
}

#[cfg(test)]
mod tests;
