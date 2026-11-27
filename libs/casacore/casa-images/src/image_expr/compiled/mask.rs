// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

impl<T: ImageExprValue> CompiledMaskExprNode<T> {
    pub(super) fn is_shape_polymorphic(&self) -> bool {
        match self {
            Self::IndexIn { .. } => true,
            Self::Not { child } | Self::BooleanValidity { child } => child.is_shape_polymorphic(),
            Self::Logical { lhs, rhs, .. } => {
                lhs.is_shape_polymorphic() || rhs.is_shape_polymorphic()
            }
            _ => false,
        }
    }

    pub(super) fn may_be_partially_invalid(&self, arena: &CompiledImageArena<T>) -> bool {
        match self {
            Self::CompareScalar { expr, .. } | Self::IsNan { child: expr } => {
                expr.may_have_mask(arena)
            }
            Self::CompareExpr { lhs, rhs, .. } => {
                lhs.may_have_mask(arena) || rhs.may_have_mask(arena)
            }
            Self::Logical { lhs, rhs, .. } => {
                lhs.may_be_partially_invalid(arena) || rhs.may_be_partially_invalid(arena)
            }
            Self::Not { child } => child.may_be_partially_invalid(arena),
            Self::ConstantMask { .. }
            | Self::Validity { .. }
            | Self::BooleanValidity { .. }
            | Self::AllReduce { .. }
            | Self::AnyReduce { .. }
            | Self::IndexIn { .. } => false,
        }
    }

    pub(super) fn eval_validity_slice(
        &self,
        arena: &CompiledImageArena<T>,
        ctx: &mut CompiledEvalContext<T>,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<Option<ArrayD<bool>>, LatticeError> {
        if !self.may_be_partially_invalid(arena) {
            return Ok(None);
        }
        match self {
            Self::CompareScalar { expr, .. } | Self::IsNan { child: expr } => {
                expr.eval_mask_slice(arena, ctx, start, shape, stride)
            }
            Self::CompareExpr {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                ..
            } => {
                let lhs_mask = eval_child_mask_slice_broadcast(
                    lhs, lhs_shape, arena, ctx, start, shape, stride,
                )?;
                let rhs_mask = eval_child_mask_slice_broadcast(
                    rhs, rhs_shape, arena, ctx, start, shape, stride,
                )?;
                Ok(combine_slice_masks(lhs_mask, rhs_mask))
            }
            Self::Logical {
                op,
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
            } => {
                let lhs_valid =
                    eval_bool_validity_broadcast(lhs, lhs_shape, arena, ctx, start, shape, stride)?;
                let rhs_valid =
                    eval_bool_validity_broadcast(rhs, rhs_shape, arena, ctx, start, shape, stride)?;
                if lhs_valid.is_none() && rhs_valid.is_none() {
                    return Ok(None);
                }
                let lhs_values = rhs_valid
                    .as_ref()
                    .map(|_| {
                        eval_bool_slice_broadcast(lhs, lhs_shape, arena, ctx, start, shape, stride)
                    })
                    .transpose()?;
                let rhs_values = lhs_valid
                    .as_ref()
                    .map(|_| {
                        eval_bool_slice_broadcast(rhs, rhs_shape, arena, ctx, start, shape, stride)
                    })
                    .transpose()?;
                let decisive = matches!(op, MaskLogicalOp::Or);
                let valid = ArrayD::from_shape_fn(IxDyn(shape), |index| {
                    let lhs_known = lhs_valid.as_ref().is_none_or(|mask| mask[&index]);
                    let rhs_known = rhs_valid.as_ref().is_none_or(|mask| mask[&index]);
                    (lhs_known && rhs_known)
                        || (lhs_known
                            && lhs_values
                                .as_ref()
                                .is_some_and(|values| values[&index] == decisive))
                        || (rhs_known
                            && rhs_values
                                .as_ref()
                                .is_some_and(|values| values[&index] == decisive))
                });
                Ok(Some(valid))
            }
            Self::Not { child } => child.eval_validity_slice(arena, ctx, start, shape, stride),
            Self::ConstantMask { .. }
            | Self::Validity { .. }
            | Self::BooleanValidity { .. }
            | Self::AllReduce { .. }
            | Self::AnyReduce { .. }
            | Self::IndexIn { .. } => Ok(None),
        }
    }

    pub(super) fn preferred_cursor_shape(
        &self,
        full_shape: &[usize],
        arena: &CompiledImageArena<T>,
        worker_limit: usize,
    ) -> Result<Vec<usize>, ImageError> {
        Ok(match self {
            Self::CompareScalar { expr, .. }
            | Self::IsNan { child: expr }
            | Self::Validity { child: expr } => {
                return expr.preferred_cursor_shape(full_shape, arena, worker_limit);
            }
            Self::CompareExpr {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                ..
            } => {
                if choose_rhs_meta(lhs_shape, rhs_shape, full_shape) {
                    return rhs.preferred_cursor_shape(full_shape, arena, worker_limit);
                }
                return lhs.preferred_cursor_shape(full_shape, arena, worker_limit);
            }
            Self::Logical {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                ..
            } => {
                if choose_rhs_meta(lhs_shape, rhs_shape, full_shape) {
                    return rhs.preferred_cursor_shape(full_shape, arena, worker_limit);
                }
                return lhs.preferred_cursor_shape(full_shape, arena, worker_limit);
            }
            Self::Not { child }
            | Self::BooleanValidity { child }
            | Self::AllReduce { child, .. }
            | Self::AnyReduce { child, .. } => {
                return child.preferred_cursor_shape(full_shape, arena, worker_limit);
            }
            Self::ConstantMask { .. } | Self::IndexIn { .. } => {
                work_balanced_cursor_shape(full_shape, worker_limit)?
            }
        })
    }

    pub(super) fn eval_slice(
        &self,
        arena: &CompiledImageArena<T>,
        ctx: &mut CompiledEvalContext<T>,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<bool>, LatticeError> {
        match self {
            Self::CompareScalar { op, expr, scalar } => {
                let data = expr.eval_slice(arena, ctx, start, shape, stride)?;
                Ok(data.mapv(|value| value.expr_compare(*scalar, *op)))
            }
            Self::CompareExpr {
                op,
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
            } => {
                let lhs_data =
                    eval_child_slice_broadcast(lhs, lhs_shape, arena, ctx, start, shape, stride)?;
                let rhs_data =
                    eval_child_slice_broadcast(rhs, rhs_shape, arena, ctx, start, shape, stride)?;
                Ok(Zip::from(&lhs_data)
                    .and(&rhs_data)
                    .map_collect(|&lhs, &rhs| lhs.expr_compare(rhs, *op)))
            }
            Self::Logical {
                op,
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
            } => {
                let lhs_mask =
                    eval_bool_slice_broadcast(lhs, lhs_shape, arena, ctx, start, shape, stride)?;
                match op {
                    MaskLogicalOp::And => {
                        if mask_chunk_none(&lhs_mask) {
                            return Ok(ArrayD::from_elem(IxDyn(shape), false));
                        }
                        if mask_chunk_all(&lhs_mask) {
                            return eval_bool_slice_broadcast(
                                rhs, rhs_shape, arena, ctx, start, shape, stride,
                            );
                        }
                    }
                    MaskLogicalOp::Or => {
                        if mask_chunk_all(&lhs_mask) {
                            return Ok(ArrayD::from_elem(IxDyn(shape), true));
                        }
                        if mask_chunk_none(&lhs_mask) {
                            return eval_bool_slice_broadcast(
                                rhs, rhs_shape, arena, ctx, start, shape, stride,
                            );
                        }
                    }
                }
                let rhs_mask =
                    eval_bool_slice_broadcast(rhs, rhs_shape, arena, ctx, start, shape, stride)?;
                let mut out = lhs_mask;
                Zip::from(out.view_mut())
                    .and(&rhs_mask)
                    .for_each(|out, &rhs| match op {
                        MaskLogicalOp::And => *out = *out && rhs,
                        MaskLogicalOp::Or => *out = *out || rhs,
                    });
                Ok(out)
            }
            Self::Not { child } => {
                let mut out = child.eval_slice(arena, ctx, start, shape, stride)?;
                out.mapv_inplace(|value| !value);
                Ok(out)
            }
            Self::IsNan { child } => {
                let data = child.eval_slice(arena, ctx, start, shape, stride)?;
                Ok(data.mapv(|value| value.expr_isnan()))
            }
            Self::Validity { child } => Ok(child
                .eval_mask_slice(arena, ctx, start, shape, stride)?
                .unwrap_or_else(|| ArrayD::from_elem(IxDyn(shape), true))),
            Self::BooleanValidity { child } => Ok(child
                .eval_validity_slice(arena, ctx, start, shape, stride)?
                .unwrap_or_else(|| ArrayD::from_elem(IxDyn(shape), true))),
            Self::ConstantMask { mask } => slice_array_owned(mask.as_ref(), start, shape, stride),
            Self::AllReduce { child, child_shape } => {
                let mut result = true;
                for_each_compiled_mask_chunk(child, child_shape, arena, |chunk| {
                    if !chunk.iter().all(|&value| value) {
                        result = false;
                    }
                    Ok(())
                })?;
                Ok(ArrayD::from_elem(IxDyn(shape), result))
            }
            Self::AnyReduce { child, child_shape } => {
                let mut result = false;
                for_each_compiled_mask_chunk(child, child_shape, arena, |chunk| {
                    if chunk.iter().any(|&value| value) {
                        result = true;
                    }
                    Ok(())
                })?;
                Ok(ArrayD::from_elem(IxDyn(shape), result))
            }
            Self::IndexIn { axis, indices } => eval_indexin(*axis, indices, start, shape, stride),
        }
    }
}
