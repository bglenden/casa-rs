// SPDX-License-Identifier: LGPL-3.0-or-later
//! Complex-valued nodes used by the mixed-pixel LEL mask parser.

use super::{
    Complex64, ExprNode, ExprParseError, ExprValueConvert, ImageExprBinaryOp, ImageExprBuilder,
    ImageExprCompareOp, ImageExprUnaryOp, ImageExprValue,
};
use crate::ImageError;
use crate::image_expr_ops::{apply_binary, apply_unary};
use casa_core::Complex32;
use std::sync::Arc;

fn error(position: usize) -> impl FnOnce(ImageError) -> ExprParseError {
    move |error| ExprParseError {
        message: error.to_string(),
        position,
    }
}

fn as_single(value: Complex64) -> Complex32 {
    Complex32::new(value.re as f32, value.im as f32)
}

fn as_double(value: Complex32) -> Complex64 {
    Complex64::new(f64::from(value.re), f64::from(value.im))
}

pub(super) fn promote<T: ImageExprValue + PartialOrd + ExprValueConvert>(
    node: ExprNode<'_, T>,
    pos: usize,
) -> Result<ImageExprBuilder<'_, Complex64>, ExprParseError> {
    match node {
        ExprNode::Complex(expr, _) => Ok(*expr),
        ExprNode::Scalar(value) => Ok(ImageExprBuilder::scalar(Complex64::new(value.value, 0.0))),
        other => other
            .into_numeric(pos)?
            .map_pixel_type(|value| Complex64::new(value.to_f64(), 0.0))
            .map_err(error(pos)),
    }
}

pub(super) fn complex_constructor<'a, T: ImageExprValue + PartialOrd + ExprValueConvert>(
    real: ExprNode<'a, T>,
    imaginary: ExprNode<'a, T>,
    position: usize,
) -> Result<ExprNode<'a, T>, ExprParseError> {
    let single = real.single_precision() && imaginary.single_precision();
    let real = real.into_numeric_or_constant(position)?;
    let imaginary = imaginary.into_numeric_or_constant(position)?;
    let promote = |value: T| Complex64::new(value.to_f64(), 0.0);
    let real = real.map_pixel_type(promote).map_err(error(position))?;
    let imaginary = imaginary.map_pixel_type(promote).map_err(error(position))?;
    real.binary_expr_with_name(
        imaginary,
        "complex",
        Arc::new(|real, imaginary| Complex64::new(real.re, imaginary.re)),
    )
    .map(|expression| ExprNode::Complex(Box::new(expression), single))
    .map_err(error(position))
}

pub(super) fn arithmetic_complex<'a, T: ImageExprValue + PartialOrd + ExprValueConvert>(
    lhs: ExprNode<'a, T>,
    rhs: ExprNode<'a, T>,
    op: ImageExprBinaryOp,
    pos: usize,
) -> Result<ExprNode<'a, T>, ExprParseError> {
    let single = lhs.single_precision() && rhs.single_precision();
    let lhs = promote(lhs, pos)?;
    let rhs = promote(rhs, pos)?;
    let expr = if single {
        lhs.binary_expr_with_name(
            rhs,
            "complex arithmetic",
            Arc::new(move |a, b| as_double(apply_binary(op, as_single(a), as_single(b)))),
        )
    } else {
        lhs.binary_expr(rhs, op)
    }
    .map_err(error(pos))?;
    Ok(ExprNode::Complex(Box::new(expr), single))
}

pub(super) fn compare_complex<'a, T: ImageExprValue + PartialOrd + ExprValueConvert>(
    lhs: ExprNode<'a, T>,
    rhs: ExprNode<'a, T>,
    op: ImageExprCompareOp,
    pos: usize,
) -> Result<ExprNode<'a, T>, ExprParseError> {
    let single = lhs.single_precision() && rhs.single_precision();
    let comparison = promote(lhs, pos)?
        .binary_expr_with_name(
            promote(rhs, pos)?,
            "complex comparison",
            Arc::new(move |a, b| {
                let value = if single {
                    as_single(a).expr_compare(as_single(b), op)
                } else {
                    a.expr_compare(b, op)
                };
                Complex64::new(f64::from(value), 0.0)
            }),
        )
        .and_then(|expression| expression.map_pixel_type(|value| T::from_f64(value.re)))
        .map_err(error(pos))?;
    Ok(ExprNode::Mask(comparison.compare_scalar(
        T::from_f64(1.0),
        ImageExprCompareOp::Equal,
    )))
}

pub(super) fn complex_function<'a, T: ImageExprValue + PartialOrd + ExprValueConvert>(
    name: &str,
    expr: ImageExprBuilder<'a, Complex64>,
    single: bool,
    op: Option<ImageExprUnaryOp>,
    pos: usize,
) -> Result<ExprNode<'a, T>, ExprParseError> {
    let convert: fn(Complex64, bool) -> f64 = match name {
        "abs" | "amplitude" => |value, single| {
            if single {
                f64::from(as_single(value).norm())
            } else {
                value.norm()
            }
        },
        "real" => |value, _| value.re,
        "imag" => |value, _| value.im,
        "arg" => |value, single| {
            if single {
                f64::from(as_single(value).arg())
            } else {
                value.arg()
            }
        },
        "value" => return Ok(ExprNode::Complex(Box::new(expr.strip_mask()), single)),
        "isnan" => {
            let real = expr
                .map_pixel_type(|value| {
                    T::from_f64(if value.expr_isnan() { f64::NAN } else { 0.0 })
                })
                .map_err(error(pos))?;
            return Ok(ExprNode::Mask(real.isnan()));
        }
        "mask" => {
            let validity = expr
                .map_pixel_type(|_| T::default_value())
                .map_err(error(pos))?;
            return Ok(ExprNode::Mask(validity.validity()));
        }
        _ => {
            if let Some(op) = op {
                let result = if single {
                    expr.custom_unary("complex unary operation", move |value| {
                        as_double(apply_unary(op, as_single(value)))
                    })
                } else {
                    expr.unary(op)
                };
                return Ok(ExprNode::Complex(Box::new(result), single));
            }
            return Err(ExprParseError {
                message: format!("unsupported complex argument to '{name}'"),
                position: pos,
            });
        }
    };
    let expr = expr
        .map_pixel_type(move |value| T::from_f64(convert(value, single)))
        .map_err(error(pos))?;
    Ok(ExprNode::numeric(expr, single))
}
