// SPDX-License-Identifier: LGPL-3.0-or-later
//! G/T cells normally contain one or two values; retain those inline.

use ndarray::{ArrayD, IxDyn};

#[derive(Debug, Clone)]
pub(super) enum GainCell<T> {
    Inline {
        values: [T; 2],
        count: usize,
        two_axes: bool,
    },
    Array(Box<ArrayD<T>>),
}

impl<T: Copy + Default> GainCell<T> {
    pub(super) fn from_slice(values: &[T], two_axes: bool) -> Self {
        if values.len() <= 2 {
            let mut inline = [T::default(); 2];
            inline[..values.len()].copy_from_slice(values);
            Self::Inline {
                values: inline,
                count: values.len(),
                two_axes,
            }
        } else {
            let shape = if two_axes {
                vec![values.len(), 1]
            } else {
                vec![values.len()]
            };
            Self::Array(Box::new(
                ArrayD::from_shape_vec(shape, values.to_vec()).expect("cell shape"),
            ))
        }
    }

    fn values(&self) -> &[T] {
        match self {
            Self::Inline { values, count, .. } => &values[..*count],
            Self::Array(array) => array
                .as_slice_memory_order()
                .expect("single-channel gains are contiguous"),
        }
    }

    pub(super) fn iter(&self) -> std::slice::Iter<'_, T> {
        self.values().iter()
    }
    pub(super) fn raw_dim(&self) -> IxDyn {
        match self {
            Self::Inline {
                count,
                two_axes: true,
                ..
            } => IxDyn(&[*count, 1]),
            Self::Inline { count, .. } => IxDyn(&[*count]),
            Self::Array(array) => array.raw_dim(),
        }
    }
    pub(super) fn mapv(&self, transform: impl FnMut(T) -> T) -> ArrayD<T> {
        ArrayD::from_shape_vec(
            self.raw_dim(),
            self.iter().copied().map(transform).collect(),
        )
        .expect("cell shape")
    }
    pub(super) fn to_array(&self) -> ArrayD<T> {
        self.mapv(std::convert::identity)
    }
}

impl<T: Copy + Default> From<ArrayD<T>> for GainCell<T> {
    fn from(array: ArrayD<T>) -> Self {
        if array.len() <= 2 && (array.ndim() == 1 || (array.ndim() == 2 && array.shape()[1] == 1)) {
            Self::from_slice(
                array
                    .as_slice_memory_order()
                    .expect("single-channel gains are contiguous"),
                array.ndim() == 2,
            )
        } else {
            Self::Array(Box::new(array))
        }
    }
}

impl<'a, T: Copy + Default> IntoIterator for &'a GainCell<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
