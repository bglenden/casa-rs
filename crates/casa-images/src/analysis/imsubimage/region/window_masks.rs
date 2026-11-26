// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::ArrayD;
use ndarray::{Array2, Axis, Dimension, IxDyn, Zip};

use super::{SavedRegionMask, SubimageSelection, shape_candidate_bounds, shape_contains};

impl SubimageSelection {
    pub(in crate::analysis::imsubimage) fn overlaps_window(
        &self,
        start: &[usize],
        shape: &[usize],
    ) -> bool {
        if self.spatial_shapes.is_empty() && self.saved_mask.is_none() {
            return true;
        }
        let [x, y] = self.plane;
        let overlaps_spatial = self.spatial_shapes.iter().any(|candidate| {
            let (x0, y0, x1, y1) = shape_candidate_bounds(
                candidate,
                self.start[x] + self.shape[x],
                self.start[y] + self.shape[y],
            );
            start[x] <= x1 && start[x] + shape[x] > x0 && start[y] <= y1 && start[y] + shape[y] > y0
        });
        overlaps_spatial
            || self.saved_mask.as_ref().is_some_and(|saved| match saved {
                SavedRegionMask::Compound(compound) => compound.overlaps(start, shape),
                SavedRegionMask::Polygon { .. } => true,
            })
    }

    pub(in crate::analysis) fn apply_selection_masks(&self, mask: &mut Option<ArrayD<bool>>) {
        self.apply_window_masks(mask, &self.start, &self.shape);
    }

    pub(in crate::analysis::imsubimage) fn apply_window_masks(
        &self,
        mask: &mut Option<ArrayD<bool>>,
        start: &[usize],
        shape: &[usize],
    ) {
        let [px, py] = self.plane;
        let box_mask = (!self.spatial_shapes.is_empty()).then(|| {
            spatial_mask(shape, [px, py], |x, y| {
                self.spatial_shapes
                    .iter()
                    .any(|spatial| shape_contains(spatial, start[px] + x, start[py] + y))
            })
        });
        let region_mask = self.saved_mask.as_ref().map(|saved| match saved {
            SavedRegionMask::Polygon {
                axes: [x, y],
                plane,
            } => spatial_mask(shape, [*x, *y], |dx, dy| {
                plane.contains(start[*x] + dx, start[*y] + dy)
            }),
            SavedRegionMask::Compound(region) => {
                let mut pixel = start.to_vec();
                ArrayD::from_shape_fn(IxDyn(shape), |idx| {
                    for ((pixel, &start), &offset) in pixel.iter_mut().zip(start).zip(idx.slice()) {
                        *pixel = start + offset;
                    }
                    region.contains(&mut pixel)
                })
            }
        });

        match (box_mask, region_mask) {
            (Some(mut bm), Some(pm)) => {
                // `box` and `region` combine by spatial union (CASA fromBCS).
                Zip::from(&mut bm).and(&pm).for_each(|b, &p| *b = *b || p);
                if !bm.iter().all(|&v| v) {
                    and_mask(mask, bm);
                }
            }
            (Some(bm), None) => {
                if !bm.iter().all(|&v| v) {
                    and_mask(mask, bm);
                }
            }
            (None, Some(pm)) => {
                if !pm.iter().all(|&v| v) {
                    and_mask(mask, pm);
                }
            }
            (None, None) => {}
        }

        if !self.qualified_shapes.is_empty() {
            let qualified = ArrayD::from_shape_fn(IxDyn(shape), |idx| {
                let x = start[px] + idx[px];
                let y = start[py] + idx[py];
                // Each containing CRTF row adds or removes the pixel in document order.
                self.qualified_shapes
                    .iter()
                    .rev()
                    .find(|region| {
                        let contains = |axis: Option<usize>, selected: Option<&[usize]>| {
                            axis.zip(selected).is_none_or(|(axis, selected)| {
                                selected.contains(&(start[axis] + idx[axis]))
                            })
                        };
                        contains(self.channel_axis, region.planes.channels.as_deref())
                            && contains(self.stokes_axis, region.planes.stokes.as_deref())
                            && shape_contains(&region.spatial, x, y)
                    })
                    .is_some_and(|region| !region.excluded)
            });
            if qualified.iter().any(|&value| !value) {
                and_mask(mask, qualified);
            }
        }

        if let Some(ax) = self.channel_axis
            && !self.channel_spans.is_empty()
        {
            mask_skipped_axis(mask, shape, ax, start[ax], |channel| {
                self.channel_spans.iter().any(|span| span.contains(channel))
            });
        }

        if let (Some(ax), Some(indices)) = (self.stokes_axis, &self.stokes_indices)
            && indices.len() != self.shape[ax]
        {
            mask_skipped_axis(mask, shape, ax, start[ax], |index| indices.contains(&index));
        }
    }
}

fn spatial_mask(
    shape: &[usize],
    [x, y]: [usize; 2],
    includes: impl Fn(usize, usize) -> bool,
) -> ArrayD<bool> {
    let plane = Array2::from_shape_fn((shape[x], shape[y]), |(x, y)| includes(x, y));
    let plane = if x < y { plane } else { plane.reversed_axes() };
    let mut mask = plane.into_dyn();
    for axis in 0..shape.len() {
        if axis != x && axis != y {
            mask = mask.insert_axis(Axis(axis));
        }
    }
    if mask.shape() == shape {
        return mask;
    }
    mask.broadcast(IxDyn(shape))
        .expect("spatial mask matches the window plane")
        .to_owned()
}

fn and_mask(dst: &mut Option<ArrayD<bool>>, rhs: ArrayD<bool>) {
    *dst = Some(match dst.take() {
        Some(mut lhs) => {
            Zip::from(&mut lhs).and(&rhs).for_each(|a, &b| *a = *a && b);
            lhs
        }
        None => rhs,
    });
}

fn mask_skipped_axis(
    mask: &mut Option<ArrayD<bool>>,
    shape: &[usize],
    axis: usize,
    start: usize,
    kept: impl Fn(usize) -> bool,
) {
    let mut m = mask
        .take()
        .unwrap_or_else(|| ArrayD::from_elem(IxDyn(shape), true));
    for (local, mut slice) in m.axis_iter_mut(Axis(axis)).enumerate() {
        if !kept(start + local) {
            slice.fill(false);
        }
    }
    *mask = Some(m);
}
