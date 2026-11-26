// SPDX-License-Identifier: LGPL-3.0-or-later

//! Copy permuted image pixels and their validity mask a tile at a time.

use ndarray::{ArrayD, Axis, IxDyn};

use casa_images::{ImageError, ImageInterface, MutableImageInterface};

use super::axes::StridedSection;

/// Copy an image into an already allocated transposed image.
///
/// `order` names the old axis at each output axis. `reverse` is indexed by
/// the old axes and selects those that should count from the far edge.
/// If the source has a default pixel mask, its transposed result is the
/// destination's default `mask0`.
///
/// # Errors
///
/// Returns an error for an invalid permutation, inconsistent output shape,
/// or an I/O failure.
pub fn copy_transposed_image(
    source: &dyn ImageInterface<f32>,
    output: &mut dyn MutableImageInterface<f32>,
    order: &[usize],
    reverse: &[bool],
) -> Result<(), ImageError> {
    let shape = source.shape();
    let rank = shape.len();
    if order.len() != rank || reverse.len() != rank {
        return Err(ImageError::CasaSelection(
            "Axis order must contain every input axis".into(),
        ));
    }
    let mut used = vec![false; rank];
    for &axis in order {
        if axis >= rank || std::mem::replace(&mut used[axis], true) {
            return Err(ImageError::CasaSelection(
                "Axis order must contain every input axis once".into(),
            ));
        }
    }
    let expected: Vec<_> = order.iter().map(|&axis| shape[axis]).collect();
    if output.shape() != expected {
        return Err(ImageError::ShapeMismatch {
            expected,
            got: output.shape().to_vec(),
        });
    }

    let zeros = vec![0; rank];
    let ones = vec![1; rank];
    let section = StridedSection {
        start: &zeros,
        shape,
        stride: &ones,
    };
    let mut mask_created = false;
    for cursor in section.cursors(source.nice_cursor_shape()) {
        let cursor = cursor?;
        let data = source.get_slice(&cursor.position, &cursor.shape, &ones)?;
        let old_start = cursor
            .position
            .iter()
            .zip(&cursor.shape)
            .enumerate()
            .map(|(axis, (&start, &count))| {
                if reverse[axis] {
                    shape[axis] - start - count
                } else {
                    start
                }
            })
            .collect::<Vec<_>>();
        let start: Vec<_> = order.iter().map(|&axis| old_start[axis]).collect();
        output.put_slice(&transpose(data, order, reverse), &start)?;
        if let Some(mask) = source.get_mask_slice(&cursor.position, &cursor.shape, &ones)? {
            if !mask_created {
                output.create_filled_mask("mask0", true)?;
                mask_created = true;
            }
            output.put_mask_slice("mask0", &transpose(mask, order, reverse), &start)?;
        }
    }
    if mask_created {
        output.set_default_mask("mask0")?;
    }
    Ok(())
}

fn transpose<T>(mut data: ArrayD<T>, order: &[usize], reverse: &[bool]) -> ArrayD<T> {
    for (axis, &flipped) in reverse.iter().enumerate() {
        if flipped {
            data.invert_axis(Axis(axis));
        }
    }
    data.permuted_axes(IxDyn(order))
}
