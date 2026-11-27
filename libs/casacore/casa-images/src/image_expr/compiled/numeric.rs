// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

impl<T: ImageExprValue> CompiledNumericExprNode<T> {
    pub(super) fn single_snapshot_mask(
        &self,
        arena: &CompiledImageArena<T>,
        full_shape: &[usize],
    ) -> Option<Arc<ArrayD<bool>>> {
        match self {
            Self::Source(source_id) => {
                let src = &arena.sources[*source_id];
                if src.shape == full_shape {
                    src.mask_snapshot.clone()
                } else {
                    None
                }
            }
            Self::UnaryOp { child, .. } | Self::CustomUnary { child, .. } => {
                child.single_snapshot_mask(arena, full_shape)
            }
            Self::BinaryOp {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                ..
            }
            | Self::CustomBinary {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                ..
            }
            | Self::SpectralIndex {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                ..
            }
            | Self::Pa {
                u: lhs,
                u_shape: lhs_shape,
                q: rhs,
                q_shape: rhs_shape,
            } => {
                if lhs_shape == full_shape && !rhs.may_have_mask(arena) {
                    lhs.single_snapshot_mask(arena, full_shape)
                } else if rhs_shape == full_shape && !lhs.may_have_mask(arena) {
                    rhs.single_snapshot_mask(arena, full_shape)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub(super) fn may_have_mask(&self, arena: &CompiledImageArena<T>) -> bool {
        match self {
            Self::Source(source_id) => arena.sources[*source_id].has_mask,
            Self::Scalar(_)
            | Self::Reduction {
                op: ReductionOp::Sum,
                ..
            }
            | Self::Reduction {
                empty_value: Some(_),
                ..
            }
            | Self::MaskCount { .. }
            | Self::StripMask { .. } => false,
            Self::UnaryOp { child, .. }
            | Self::CustomUnary { child, .. }
            | Self::Reduction { child, .. }
            | Self::Fractile { child, .. }
            | Self::FractileRange { child, .. }
            | Self::Rebin { child, .. } => child.may_have_mask(arena),
            Self::BinaryOp { lhs, rhs, .. }
            | Self::CustomBinary { lhs, rhs, .. }
            | Self::SpectralIndex { lhs, rhs, .. }
            | Self::Pa { u: lhs, q: rhs, .. } => {
                lhs.may_have_mask(arena) || rhs.may_have_mask(arena)
            }
            Self::Conditional {
                condition,
                if_true,
                if_false,
                ..
            } => {
                condition.may_be_partially_invalid(arena)
                    || if_true.may_have_mask(arena)
                    || if_false.may_have_mask(arena)
            }
            Self::Replace { primary, .. } => primary.may_have_mask(arena),
        }
    }

    pub(super) fn preferred_cursor_shape(
        &self,
        full_shape: &[usize],
        arena: &CompiledImageArena<T>,
        worker_limit: usize,
    ) -> Result<Vec<usize>, ImageError> {
        Ok(match self {
            Self::Source(source_id) => {
                let source = &arena.sources[*source_id];
                if source.shape != full_shape {
                    return work_balanced_cursor_shape(full_shape, worker_limit);
                }
                match &source.tile_shape {
                    Some(tile_shape) => expand_tile_aligned_cursor_shape(
                        full_shape,
                        tile_shape,
                        work_balanced_pixel_target(full_shape, worker_limit)?,
                    )?,
                    None => work_balanced_cursor_shape(full_shape, worker_limit)?,
                }
            }
            Self::UnaryOp { child, .. }
            | Self::CustomUnary { child, .. }
            | Self::StripMask { child }
            | Self::Reduction { child, .. }
            | Self::Fractile { child, .. }
            | Self::FractileRange { child, .. } => {
                return child.preferred_cursor_shape(full_shape, arena, worker_limit);
            }
            Self::BinaryOp {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                ..
            }
            | Self::CustomBinary {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                ..
            }
            | Self::SpectralIndex {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                ..
            }
            | Self::Pa {
                u: lhs,
                u_shape: lhs_shape,
                q: rhs,
                q_shape: rhs_shape,
            } => {
                if choose_rhs_meta(lhs_shape, rhs_shape, full_shape) {
                    return rhs.preferred_cursor_shape(full_shape, arena, worker_limit);
                }
                return lhs.preferred_cursor_shape(full_shape, arena, worker_limit);
            }
            Self::Conditional {
                if_true,
                true_shape,
                if_false,
                false_shape,
                ..
            } => {
                if choose_rhs_meta(true_shape, false_shape, full_shape) {
                    return if_false.preferred_cursor_shape(full_shape, arena, worker_limit);
                }
                return if_true.preferred_cursor_shape(full_shape, arena, worker_limit);
            }
            Self::Replace { primary, .. } => {
                return primary.preferred_cursor_shape(full_shape, arena, worker_limit);
            }
            Self::Scalar(_) | Self::MaskCount { .. } | Self::Rebin { .. } => {
                work_balanced_cursor_shape(full_shape, worker_limit)?
            }
        })
    }

    pub(super) fn eval_mask_slice(
        &self,
        arena: &CompiledImageArena<T>,
        ctx: &mut CompiledEvalContext<T>,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<Option<ArrayD<bool>>, LatticeError> {
        if !self.may_have_mask(arena) {
            return Ok(None);
        }
        match self {
            Self::Source(source_id) => arena.sources[*source_id]
                .eval_mask_slice(arena, ctx, *source_id, start, shape, stride),
            Self::Scalar(_)
            | Self::Reduction {
                op: ReductionOp::Sum,
                ..
            }
            | Self::MaskCount { .. }
            | Self::StripMask { .. } => Ok(None),
            Self::Reduction {
                child, child_shape, ..
            }
            | Self::Fractile {
                child, child_shape, ..
            }
            | Self::FractileRange {
                child, child_shape, ..
            } => {
                let valid = child.any_valid_pixel(arena, ctx, child_shape)?;
                Ok((!valid).then(|| ArrayD::from_elem(IxDyn(shape), false)))
            }
            Self::UnaryOp { child, .. } | Self::CustomUnary { child, .. } => {
                child.eval_mask_slice(arena, ctx, start, shape, stride)
            }
            Self::BinaryOp {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                ..
            }
            | Self::CustomBinary {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                ..
            }
            | Self::SpectralIndex {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                ..
            }
            | Self::Pa {
                u: lhs,
                u_shape: lhs_shape,
                q: rhs,
                q_shape: rhs_shape,
            } => {
                let lhs_mask = eval_child_mask_slice_broadcast(
                    lhs, lhs_shape, arena, ctx, start, shape, stride,
                )?;
                let rhs_mask = eval_child_mask_slice_broadcast(
                    rhs, rhs_shape, arena, ctx, start, shape, stride,
                )?;
                Ok(combine_slice_masks(lhs_mask, rhs_mask))
            }
            Self::Replace {
                primary,
                primary_shape,
                ..
            } => eval_child_mask_slice_broadcast(
                primary,
                primary_shape,
                arena,
                ctx,
                start,
                shape,
                stride,
            ),
            Self::Conditional {
                condition,
                cond_shape,
                if_true,
                true_shape,
                if_false,
                false_shape,
            } => {
                let cond_vals = eval_bool_slice_broadcast(
                    condition, cond_shape, arena, ctx, start, shape, stride,
                )?;
                let cond_valid = eval_bool_validity_broadcast(
                    condition, cond_shape, arena, ctx, start, shape, stride,
                )?;
                let true_valid = eval_child_mask_slice_broadcast(
                    if_true, true_shape, arena, ctx, start, shape, stride,
                )?;
                let false_valid = eval_child_mask_slice_broadcast(
                    if_false,
                    false_shape,
                    arena,
                    ctx,
                    start,
                    shape,
                    stride,
                )?;
                if cond_valid.is_none() && true_valid.is_none() && false_valid.is_none() {
                    return Ok(None);
                }
                let mut out = ArrayD::from_elem(IxDyn(shape), true);
                for (pos, dst) in out.indexed_iter_mut() {
                    let cv = cond_valid.as_ref().is_none_or(|m| m[&pos]);
                    let tv = if cond_vals[&pos] {
                        true_valid.as_ref().is_none_or(|m| m[&pos])
                    } else {
                        false_valid.as_ref().is_none_or(|m| m[&pos])
                    };
                    *dst = cv && tv;
                }
                Ok(Some(out))
            }
            Self::Rebin {
                child,
                in_shape,
                factors,
            } => {
                let (c_start, c_shape, c_stride) =
                    rebin_child_slice(in_shape, factors, start, shape, stride);
                let Some(child_mask) =
                    child.eval_mask_slice(arena, ctx, &c_start, &c_shape, &c_stride)?
                else {
                    return Ok(None);
                };
                Ok(Some(rebin_mask_slice(&child_mask, factors, shape, stride)))
            }
        }
    }

    fn any_valid_pixel(
        &self,
        arena: &CompiledImageArena<T>,
        ctx: &mut CompiledEvalContext<T>,
        shape: &[usize],
    ) -> Result<bool, LatticeError> {
        if !self.may_have_mask(arena) {
            return Ok(true);
        }
        if shape.is_empty() {
            return Ok(self
                .eval_mask_slice(arena, ctx, &[], &[], &[])?
                .is_none_or(|mask| mask[IxDyn(&[])]));
        }
        let cursor = clamp_cursor_shape(
            &self
                .preferred_cursor_shape(shape, arena, thread_parallelism())
                .map_err(|error| LatticeError::InvalidTraversal(error.to_string()))?,
            shape,
        );
        for cursor in TraversalCursorIter::new(
            shape.to_vec(),
            cursor.clone(),
            TraversalSpec::chunks(cursor),
        ) {
            let cursor = cursor?;
            let stride = vec![1; shape.len()];
            if self
                .eval_mask_slice(arena, ctx, &cursor.position, &cursor.shape, &stride)?
                .is_none_or(|mask| mask.iter().any(|&valid| valid))
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(super) fn eval_slice(
        &self,
        arena: &CompiledImageArena<T>,
        ctx: &mut CompiledEvalContext<T>,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError> {
        match self {
            Self::Source(source_id) => {
                arena.sources[*source_id].eval_slice(arena, ctx, *source_id, start, shape, stride)
            }
            Self::Scalar(value) => Ok(ArrayD::from_elem(IxDyn(shape), *value)),
            Self::UnaryOp { op, child } => {
                let mut data = child.eval_slice(arena, ctx, start, shape, stride)?;
                data.mapv_inplace(|value| apply_unary(*op, value));
                Ok(data)
            }
            Self::StripMask { child } => child.eval_slice(arena, ctx, start, shape, stride),
            Self::BinaryOp {
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
                    .map_collect(|&lhs, &rhs| apply_binary(*op, lhs, rhs)))
            }
            Self::CustomUnary { child, func } => {
                let mut data = child.eval_slice(arena, ctx, start, shape, stride)?;
                let BinaryOrUnary::Unary(func) = func else {
                    unreachable!("custom unary node stored binary function")
                };
                data.mapv_inplace(|value| func(value));
                Ok(data)
            }
            Self::CustomBinary {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                func,
            } => {
                let lhs_data =
                    eval_child_slice_broadcast(lhs, lhs_shape, arena, ctx, start, shape, stride)?;
                let rhs_data =
                    eval_child_slice_broadcast(rhs, rhs_shape, arena, ctx, start, shape, stride)?;
                let BinaryOrUnary::Binary(func) = func else {
                    unreachable!("custom binary node stored unary function")
                };
                Ok(Zip::from(&lhs_data)
                    .and(&rhs_data)
                    .map_collect(|&lhs, &rhs| func(lhs, rhs)))
            }
            Self::SpectralIndex {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                spec_axis_a,
                freqs_a,
                spec_axis_b,
                freqs_b,
            } => {
                let lhs_data =
                    eval_child_slice_broadcast(lhs, lhs_shape, arena, ctx, start, shape, stride)?;
                let rhs_data =
                    eval_child_slice_broadcast(rhs, rhs_shape, arena, ctx, start, shape, stride)?;
                Ok(eval_spectral_index(
                    lhs_data,
                    rhs_data,
                    *spec_axis_a,
                    freqs_a,
                    *spec_axis_b,
                    freqs_b,
                    start,
                    stride,
                ))
            }
            Self::Pa {
                u,
                u_shape,
                q,
                q_shape,
            } => {
                let u_data =
                    eval_child_slice_broadcast(u, u_shape, arena, ctx, start, shape, stride)?;
                let q_data =
                    eval_child_slice_broadcast(q, q_shape, arena, ctx, start, shape, stride)?;
                Ok(eval_pa(&u_data, &q_data))
            }
            Self::Rebin {
                child,
                in_shape,
                factors,
            } => {
                let (c_start, c_shape, c_stride) =
                    rebin_child_slice(in_shape, factors, start, shape, stride);
                let in_vals = child.eval_slice(arena, ctx, &c_start, &c_shape, &c_stride)?;
                let in_mask = child.eval_mask_slice(arena, ctx, &c_start, &c_shape, &c_stride)?;
                let (out_vals, _) =
                    rebin_reduce_slice(&in_vals, in_mask.as_ref(), factors, shape, stride);
                Ok(out_vals)
            }
            Self::Reduction {
                op,
                child,
                child_shape,
                empty_value,
            } => Ok(ArrayD::from_elem(
                IxDyn(shape),
                reduce_numeric_child(*op, child, child_shape, arena, *empty_value)?,
            )),
            Self::Fractile {
                child,
                child_shape,
                fraction,
            } => {
                let mut values = collect_numeric_child(child, child_shape, arena)?;
                let value =
                    fractile_in_place(&mut values, *fraction).unwrap_or_else(T::default_value);
                Ok(ArrayD::from_elem(IxDyn(shape), value))
            }
            Self::FractileRange {
                child,
                child_shape,
                fraction1,
                fraction2,
            } => {
                let mut values = collect_numeric_child(child, child_shape, arena)?;
                let mut clone_vals = values.clone();
                let v1 =
                    fractile_in_place(&mut clone_vals, *fraction1).unwrap_or_else(T::default_value);
                let v2 =
                    fractile_in_place(&mut values, *fraction2).unwrap_or_else(T::default_value);
                Ok(ArrayD::from_elem(IxDyn(shape), v2 - v1))
            }
            Self::Conditional {
                condition,
                cond_shape,
                if_true,
                true_shape,
                if_false,
                false_shape,
            } => eval_conditional_slice(
                condition,
                cond_shape,
                if_true,
                true_shape,
                if_false,
                false_shape,
                arena,
                ctx,
                start,
                shape,
                stride,
            ),
            Self::MaskCount {
                count_true,
                mask,
                mask_shape,
            } => {
                let mut count = 0usize;
                for_each_compiled_mask_chunk(mask, mask_shape, arena, |chunk| {
                    count += if *count_true {
                        chunk.iter().filter(|&&value| value).count()
                    } else {
                        chunk.iter().filter(|&&value| !value).count()
                    };
                    Ok(())
                })?;
                Ok(ArrayD::from_elem(
                    IxDyn(shape),
                    T::from_f64_lossy(count as f64),
                ))
            }
            Self::Replace {
                primary,
                primary_shape,
                replacement,
                replacement_shape,
                mask,
            } => eval_replace_slice(
                primary,
                primary_shape,
                replacement,
                replacement_shape,
                mask.as_deref(),
                arena,
                ctx,
                start,
                shape,
                stride,
            ),
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn eval_conditional_slice<T: ImageExprValue>(
    condition: &CompiledMaskExprNode<T>,
    cond_shape: &[usize],
    if_true: &CompiledNumericExprNode<T>,
    true_shape: &[usize],
    if_false: &CompiledNumericExprNode<T>,
    false_shape: &[usize],
    arena: &CompiledImageArena<T>,
    ctx: &mut CompiledEvalContext<T>,
    start: &[usize],
    shape: &[usize],
    stride: &[usize],
) -> Result<ArrayD<T>, LatticeError> {
    let cond = eval_bool_slice_broadcast(condition, cond_shape, arena, ctx, start, shape, stride)?;
    let true_data =
        eval_child_slice_broadcast(if_true, true_shape, arena, ctx, start, shape, stride)?;
    let false_data =
        eval_child_slice_broadcast(if_false, false_shape, arena, ctx, start, shape, stride)?;
    Ok(Zip::from(&cond)
        .and(&true_data)
        .and(&false_data)
        .map_collect(|&condition, &t_val, &f_val| if condition { t_val } else { f_val }))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn eval_replace_slice<T: ImageExprValue>(
    primary: &CompiledNumericExprNode<T>,
    primary_shape: &[usize],
    replacement: &CompiledNumericExprNode<T>,
    replacement_shape: &[usize],
    mask: Option<&ArrayD<bool>>,
    arena: &CompiledImageArena<T>,
    ctx: &mut CompiledEvalContext<T>,
    start: &[usize],
    shape: &[usize],
    stride: &[usize],
) -> Result<ArrayD<T>, LatticeError> {
    let mask_data = match mask {
        Some(mask) => Some(slice_array_owned(mask, start, shape, stride)?),
        None => eval_child_mask_slice_broadcast(
            primary,
            primary_shape,
            arena,
            ctx,
            start,
            shape,
            stride,
        )?,
    };
    let Some(mask_data) = mask_data.filter(|mask| !mask_chunk_all(mask)) else {
        return eval_child_slice_broadcast(
            primary,
            primary_shape,
            arena,
            ctx,
            start,
            shape,
            stride,
        );
    };
    if mask_chunk_none(&mask_data) {
        return eval_child_slice_broadcast(
            replacement,
            replacement_shape,
            arena,
            ctx,
            start,
            shape,
            stride,
        );
    }
    let mut out =
        eval_child_slice_broadcast(primary, primary_shape, arena, ctx, start, shape, stride)?;
    let replacement_data = eval_child_slice_broadcast(
        replacement,
        replacement_shape,
        arena,
        ctx,
        start,
        shape,
        stride,
    )?;
    Zip::from(&mask_data)
        .and(out.view_mut())
        .and(&replacement_data)
        .for_each(|&mask, out, &replacement| {
            if !mask {
                *out = replacement;
            }
        });
    Ok(out)
}
