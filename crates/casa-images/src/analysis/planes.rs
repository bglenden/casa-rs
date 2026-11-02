// SPDX-License-Identifier: LGPL-3.0-or-later
//! Planes of an image: `feather`'s direction planes and `impv`'s samples
//! along a path in one, with the plane on any two axes (#1002).

use casa_types::ArrayD;
use ndarray::{Array2, IxDyn, ShapeBuilder};

use super::axes::other_axes;

/// One plane of an image: its two `axes`, at `values` of the `others`.
pub(super) struct PlaneAt<'a> {
    pub(super) axes: [usize; 2],
    pub(super) others: &'a [usize],
    pub(super) values: &'a [usize],
}

impl PlaneAt<'_> {
    /// The index of the plane's pixel (`x`, `y`) in the image.
    fn index(&self, rank: usize, x: usize, y: usize) -> Vec<usize> {
        let mut index = vec![0; rank];
        for (&axis, &value) in self.others.iter().zip(self.values) {
            index[axis] = value;
        }
        index[self.axes[0]] = x;
        index[self.axes[1]] = y;
        index
    }

    pub(super) fn extract(&self, data: &ArrayD<f32>) -> Array2<f32> {
        let shape = (data.shape()[self.axes[0]], data.shape()[self.axes[1]]);
        Array2::from_shape_fn(shape, |(x, y)| data[IxDyn(&self.index(data.ndim(), x, y))])
    }

    pub(super) fn put(&self, data: &mut ArrayD<f32>, plane: &Array2<f32>) {
        for ((x, y), value) in plane.indexed_iter() {
            let index = self.index(data.ndim(), x, y);
            data[IxDyn(&index)] = *value;
        }
    }
}

/// The index of element `value` of an array of `shape`, the last axis
/// fastest.
pub(super) fn unravel_index(mut value: usize, shape: &[usize]) -> Vec<usize> {
    if shape.is_empty() {
        return Vec::new();
    }
    let mut out = vec![0usize; shape.len()];
    for axis in (0..shape.len()).rev() {
        out[axis] = value % shape[axis];
        value /= shape[axis];
    }
    out
}

/// impv's samples: at each pixel of `path` in the image plane `plane`, the
/// mean of `input` over `offsets` about it that fall in the image, for every
/// pixel of the other axes. Output channel `j` of `channels`' axis comes from
/// input channel `channels.1[j]`.
pub(super) fn pv_samples<T>(
    input: &ArrayD<T>,
    plane: [usize; 2],
    path: &[(isize, isize)],
    offsets: &[(isize, isize)],
    channels: Option<&(usize, Vec<usize>)>,
    output_shape: &[usize],
) -> ArrayD<f32>
where
    T: Into<f64> + Copy,
{
    let [x_axis, y_axis] = plane;
    let others = other_axes(output_shape.len(), plane);
    let other_shape = others
        .iter()
        .map(|&axis| output_shape[axis])
        .collect::<Vec<_>>();
    let mut output = ArrayD::<f32>::zeros(IxDyn(output_shape).f());
    let mut input_index = vec![0; input.ndim()];
    let mut output_index = vec![0; output_shape.len()];
    for sample_index in all_indices_for_shape(&other_shape) {
        for (&axis, &value) in others.iter().zip(&sample_index) {
            output_index[axis] = value;
            input_index[axis] = match channels {
                Some((channel_axis, local)) if *channel_axis == axis => local[value],
                _ => value,
            };
        }
        for (path_index, &(x, y)) in path.iter().enumerate() {
            let mut sum = 0.0;
            let mut count = 0usize;
            for &(dx, dy) in offsets {
                let (Ok(sx), Ok(sy)) = (usize::try_from(x + dx), usize::try_from(y + dy)) else {
                    continue;
                };
                if sx >= input.shape()[x_axis] || sy >= input.shape()[y_axis] {
                    continue;
                }
                input_index[x_axis] = sx;
                input_index[y_axis] = sy;
                sum += input[IxDyn(&input_index)].into();
                count += 1;
            }
            if count > 0 {
                output_index[x_axis] = path_index;
                output_index[y_axis] = 0;
                output[IxDyn(&output_index)] = (sum / count as f64) as f32;
            }
        }
    }
    output
}

fn all_indices_for_shape(shape: &[usize]) -> Vec<Vec<usize>> {
    fn push(shape: &[usize], axis: usize, current: &mut Vec<usize>, out: &mut Vec<Vec<usize>>) {
        if axis == shape.len() {
            out.push(current.clone());
            return;
        }
        for value in 0..shape[axis] {
            current.push(value);
            push(shape, axis + 1, current, out);
            current.pop();
        }
    }
    let mut out = Vec::new();
    push(shape, 0, &mut Vec::new(), &mut out);
    out
}
