// SPDX-License-Identifier: LGPL-3.0-or-later

//! Region plane compaction for `ia.statistics`.

use casa_core::{ArrayValue, RecordValue, Value};
use ndarray::{ArrayD, Axis};

/// Display planes omitted entirely by the geometric selection.
pub(super) struct RegionPlanes {
    shape: Vec<usize>,
    selections: Vec<(Axis, Vec<usize>)>,
}

impl RegionPlanes {
    /// Builds a plane selector dropping display slices where `membership` has no `true` entries.
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

    /// Removes excluded planes from per-plane `Float64` arrays inside `record`.
    pub(super) fn compact(&self, record: &mut RecordValue) {
        let Some(((axis, included), rest)) = self.selections.split_first() else {
            return;
        };
        for field in record.fields_mut() {
            let Value::Array(ArrayValue::Float64(array)) = &mut field.value else {
                continue;
            };
            if array.shape() != self.shape.as_slice() || array.is_empty() {
                continue;
            }
            let mut compacted = array.select(*axis, included);
            for (axis, included) in rest {
                compacted = compacted.select(*axis, included);
            }
            *array = compacted;
        }
    }
}
