// SPDX-License-Identifier: LGPL-3.0-or-later
//! Planes of an image: `feather`'s direction planes and `impv`'s samples
//! along a path in one, with the plane on any two axes (#1002).

use casa_types::ArrayD;
use ndarray::{Array2, IxDyn};

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
