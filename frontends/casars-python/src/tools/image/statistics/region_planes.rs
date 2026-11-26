// SPDX-License-Identifier: LGPL-3.0-or-later

use ndarray::{ArrayD, ArrayViewD, Axis, IxDyn, ShapeBuilder};

use crate::convert::value::CasaValue;

/// Display planes omitted entirely by the geometric selection.
pub(super) struct RegionPlanes {
    shape: Vec<usize>,
    selections: Vec<(Axis, Vec<usize>)>,
}

impl RegionPlanes {
    pub(super) fn from_membership(membership: &ArrayD<bool>, axes: &[usize]) -> Option<Self> {
        if axes.is_empty() {
            return None;
        }
        let mut shape = Vec::new();
        let mut selections = Vec::new();
        for (axis, &length) in membership.shape().iter().enumerate() {
            if axes.contains(&axis) || length == 1 {
                continue;
            }
            let included: Vec<_> = membership
                .axis_iter(Axis(axis))
                .enumerate()
                .filter_map(|(index, plane)| plane.iter().any(|&value| value).then_some(index))
                .collect();
            if included.len() < length {
                selections.push((Axis(shape.len()), included));
            }
            shape.push(length);
        }
        (!selections.is_empty()).then_some(Self { shape, selections })
    }

    pub(super) fn compact(&self, result: &mut CasaValue) {
        let CasaValue::Record(fields) = result else {
            return;
        };
        let Some(((axis, included), rest)) = self.selections.split_first() else {
            return;
        };
        for (_, value) in fields {
            let CasaValue::Floats { shape, values } = value else {
                continue;
            };
            if *shape != self.shape || values.is_empty() {
                continue;
            }
            let array = ArrayViewD::from_shape(IxDyn(shape).f(), values)
                .expect("a statistics field has one value per display plane");
            let mut compacted = array.select(*axis, included);
            for (axis, included) in rest {
                compacted = compacted.select(*axis, included);
            }
            *shape = compacted.shape().to_vec();
            *values = compacted.reversed_axes().iter().copied().collect();
        }
    }
}
