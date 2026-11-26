// SPDX-License-Identifier: LGPL-3.0-or-later
//! Canonical image-expression operator semantics.

use crate::{ImageExprBinaryOp, ImageExprUnaryOp, image_expr::ImageExprValue};

pub(crate) fn apply_unary<T: ImageExprValue>(op: ImageExprUnaryOp, value: T) -> T {
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

pub(crate) fn apply_binary<T: ImageExprValue>(op: ImageExprBinaryOp, lhs: T, rhs: T) -> T {
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
