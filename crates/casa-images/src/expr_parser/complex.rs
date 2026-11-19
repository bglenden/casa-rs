// SPDX-License-Identifier: LGPL-3.0-or-later
//! Complex-valued nodes used by the mixed-pixel LEL mask parser.

use super::{
    Complex64, ExprNode, ExprParseError, ExprValueConvert, ImageExprBinaryOp, ImageExprBuilder,
    ImageExprCompareOp, ImageExprUnaryOp, ImageExprValue, MaskExprBuilder,
};
use crate::ImageError;

fn error(position: usize) -> impl FnOnce(ImageError) -> ExprParseError {
    move |error| ExprParseError {
        message: error.to_string(),
        position,
    }
}

fn promote<T: ImageExprValue + PartialOrd + ExprValueConvert>(
    node: ExprNode<'_, T>,
    pos: usize,
) -> Result<ImageExprBuilder<'_, Complex64>, ExprParseError> {
    match node {
        ExprNode::Complex(expr) => Ok(*expr),
        ExprNode::Scalar(value) => Ok(ImageExprBuilder::scalar(Complex64::new(value, 0.0))),
        other => other
            .into_numeric(pos)?
            .map_pixel_type(|value| Complex64::new(value.to_f64(), 0.0))
            .map_err(error(pos)),
    }
}

pub(super) fn arithmetic_complex<'a, T: ImageExprValue + PartialOrd + ExprValueConvert>(
    lhs: ExprNode<'a, T>,
    rhs: ExprNode<'a, T>,
    op: ImageExprBinaryOp,
    pos: usize,
) -> Result<ExprNode<'a, T>, ExprParseError> {
    promote(lhs, pos)?
        .binary_expr(promote(rhs, pos)?, op)
        .map(|expr| ExprNode::Complex(Box::new(expr)))
        .map_err(error(pos))
}

pub(super) fn compare_complex<'a, T: ImageExprValue + PartialOrd + ExprValueConvert>(
    lhs: ExprNode<'a, T>,
    rhs: ExprNode<'a, T>,
    op: ImageExprCompareOp,
    pos: usize,
) -> Result<ExprNode<'a, T>, ExprParseError> {
    let mask = promote(lhs, pos)?
        .compare_expr(promote(rhs, pos)?, op)
        .map_err(error(pos))?;
    let data = mask
        .compile()
        .and_then(|mask| mask.get())
        .map_err(error(pos))?;
    Ok(ExprNode::Mask(MaskExprBuilder::from_constant(data)))
}

pub(super) fn complex_function<'a, T: ImageExprValue + PartialOrd + ExprValueConvert>(
    name: &str,
    expr: ImageExprBuilder<'a, Complex64>,
    op: Option<ImageExprUnaryOp>,
    pos: usize,
) -> Result<ExprNode<'a, T>, ExprParseError> {
    let convert: fn(Complex64) -> f64 = match name {
        "abs" | "amplitude" => Complex64::norm,
        "real" => |value| value.re,
        "imag" => |value| value.im,
        "arg" => Complex64::arg,
        "value" => return Ok(ExprNode::Complex(Box::new(expr.strip_mask()))),
        "mask" => {
            let mask = expr
                .source_mask()
                .map_err(error(pos))?
                .unwrap_or_else(|| ndarray::ArrayD::from_elem(ndarray::IxDyn(expr.shape()), true));
            return Ok(ExprNode::Mask(MaskExprBuilder::from_constant(mask)));
        }
        _ if op.is_some() => return Ok(ExprNode::Complex(Box::new(expr.unary(op.unwrap())))),
        _ => {
            return Err(ExprParseError {
                message: format!("unsupported complex argument to '{name}'"),
                position: pos,
            });
        }
    };
    let expr = expr
        .map_pixel_type(move |value| T::from_f64(convert(value)))
        .map_err(error(pos))?;
    Ok(ExprNode::Numeric(Box::new(expr)))
}
