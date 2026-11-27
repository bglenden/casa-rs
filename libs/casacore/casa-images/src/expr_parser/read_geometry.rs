// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's lattice geometry checks for Boolean masks on public image reads.

use casa_coordinates::CoordinateSystem;

use crate::image_expr::ImageExprValue;
use crate::image_expr_lel::{can_broadcast_to, mask_coordinate_axes};

use super::{ExprNode, ExprParseError, ExprValueConvert, ImageResolver, Parser};

#[derive(Clone, Copy)]
pub(super) enum Operator {
    Infix,
    Logical,
    Function,
}

impl<T: ImageExprValue + PartialOrd + ExprValueConvert> ExprNode<'_, T> {
    fn lattice_geometry(&self) -> Option<(&[usize], Option<&CoordinateSystem>)> {
        let (shape, coordinates) = match self {
            Self::Numeric(image, _) => (image.shape(), image.expression_coordinates()),
            Self::Complex(image, _) => (image.shape(), image.expression_coordinates()),
            Self::Mask(mask) => (mask.shape(), mask.expression_coordinates()),
            Self::Scalar(_) | Self::IntArray(_) => return None,
        };
        (!shape.is_empty()).then_some((shape, coordinates))
    }
}

impl<'a, T, R> Parser<'a, '_, T, R>
where
    T: ImageExprValue + PartialOrd + ExprValueConvert,
    R: ImageResolver<'a, T>,
{
    pub(super) fn validate_lattice_operands(
        &self,
        lhs: &ExprNode<'a, T>,
        rhs: &ExprNode<'a, T>,
        position: usize,
        operator: Operator,
    ) -> Result<(), ExprParseError> {
        if !self.check_lattice_geometry {
            return Ok(());
        }
        let (Some((left, left_coordinates)), Some((right, right_coordinates))) =
            (lhs.lattice_geometry(), rhs.lattice_geometry())
        else {
            return Ok(());
        };
        let rejected = |message: &str| ExprParseError {
            message: if matches!(operator, Operator::Logical) {
                "LatticeExprNode::resultDataType - Bool and numeric operands cannot mixed".into()
            } else {
                message.into()
            },
            position,
        };
        if matches!(operator, Operator::Function) && left != right {
            return Err(rejected("LELAttribute: shapes of operands mismatch"));
        }
        let right_is_smaller = can_broadcast_to(right, left);
        let left_is_smaller = can_broadcast_to(left, right);
        if !left_is_smaller && !right_is_smaller {
            return Err(rejected("LatticeExprNode - shapes of operands mismatch"));
        }
        let (small, large) = if right_is_smaller {
            (right, left)
        } else {
            (left, right)
        };
        if let (Some(left), Some(right)) = (left_coordinates, right_coordinates) {
            let left = mask_coordinate_axes(left);
            let right = mask_coordinate_axes(right);
            if !(left.starts_with(&right) || right.starts_with(&left)) {
                return Err(rejected(
                    "LatticeExprNode - coordinates of operands mismatch",
                ));
            }
        }
        if small != large && small.iter().all(|&length| length == 1) {
            return Err(rejected("ExtendSpecifier - no axes remaining"));
        }
        Ok(())
    }
}
