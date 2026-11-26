// SPDX-License-Identifier: LGPL-3.0-or-later
//! Two-axis views of an image, with the other axes numbered first-fastest.

use casa_core::ArrayD;
use ndarray::{ArrayBase, ArrayView2, ArrayViewMut2, Axis, Data, Ix2, IxDyn};

pub(crate) struct PlaneIndex<'s> {
    shape: &'s [usize],
    axes: [usize; 2],
    others: Vec<usize>,
}

impl<'s> PlaneIndex<'s> {
    pub(crate) fn new(shape: &'s [usize], axes: [usize; 2]) -> Self {
        let others = (0..shape.len())
            .filter(|axis| !axes.contains(axis))
            .collect();
        Self {
            shape,
            axes,
            others,
        }
    }

    pub(crate) fn count(&self) -> usize {
        self.others.iter().map(|&axis| self.shape[axis]).product()
    }

    pub(crate) fn pixel(&self, plane: usize, axis: usize) -> usize {
        if self.axes.contains(&axis) {
            return 0;
        }
        let stride: usize = self
            .others
            .iter()
            .take_while(|&&other| other < axis)
            .map(|&other| self.shape[other])
            .product();
        plane / stride % self.shape[axis]
    }

    pub(crate) fn view<'a, A>(&self, plane: usize, array: &'a ArrayD<A>) -> ArrayView2<'a, A> {
        self.select(plane, array.view())
    }

    pub(crate) fn view_mut<'a, A>(
        &self,
        plane: usize,
        array: &'a mut ArrayD<A>,
    ) -> ArrayViewMut2<'a, A> {
        self.select(plane, array.view_mut())
    }

    fn select<S: Data>(&self, plane: usize, mut view: ArrayBase<S, IxDyn>) -> ArrayBase<S, Ix2> {
        // Removing axes from highest to lowest leaves the remaining axis indices valid.
        for &axis in self.others.iter().rev() {
            view = view.index_axis_move(Axis(axis), self.pixel(plane, axis));
        }
        let view = view.into_dimensionality::<Ix2>().expect("two plane axes");
        if self.axes[0] < self.axes[1] {
            view
        } else {
            view.reversed_axes()
        }
    }

    pub(crate) fn read<A, B>(
        &self,
        plane: usize,
        array: &ArrayD<A>,
        into: &mut [B],
        convert: impl Fn(&A) -> B,
    ) {
        let view = self.view(plane, array).reversed_axes();
        if let Some(values) = view.as_slice() {
            for (out, value) in into.iter_mut().zip(values) {
                *out = convert(value);
            }
        } else {
            for (out, value) in into.iter_mut().zip(view) {
                *out = convert(value);
            }
        }
    }

    pub(crate) fn write<A: Copy>(&self, plane: usize, values: &[A], array: &mut ArrayD<A>) {
        let mut view = self.view_mut(plane, array).reversed_axes();
        if let Some(out) = view.as_slice_mut() {
            out.copy_from_slice(values);
        } else {
            for (out, value) in view.into_iter().zip(values) {
                *out = *value;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::ShapeBuilder;

    #[test]
    fn views_keep_coordinate_order_with_reversed_interleaved_axes() {
        let shape = [2, 3, 2, 4];
        let data = ArrayD::from_shape_fn(IxDyn(&shape).f(), |i| {
            (i[0] + 10 * i[1] + 100 * i[2] + 1000 * i[3]) as i32
        });
        let planes = PlaneIndex::new(&shape, [3, 1]);
        let mut output = ArrayD::zeros(IxDyn(&shape).f());
        let mut buffer = [0; 12];
        assert_eq!(planes.count(), 4);
        for plane in 0..planes.count() {
            planes.read(plane, &data, &mut buffer, |v| *v);
            assert_eq!(buffer[5], 1010 + (plane % 2 + 100 * (plane / 2)) as i32);
            planes.write(plane, &buffer, &mut output);
        }
        assert_eq!(output, data);
    }
}
