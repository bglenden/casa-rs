// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_types::ArrayD;
use ndarray::{Axis, IxDyn, Slice, Zip};

use super::{SavedRegionMask, SubimageSelection, shape_contains};

impl SubimageSelection {
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
        let box_mask = self.spatial_mask.as_ref().map(|spatial| {
            let x = start[px] - self.start[px];
            let y = start[py] - self.start[py];
            ArrayD::from_shape_fn(IxDyn(shape), |idx| spatial[[x + idx[px], y + idx[py]]])
        });
        let region_mask = self.saved_mask.as_ref().map(|saved| {
            let (pixels, cropped) = match saved {
                SavedRegionMask::Whole(pixels) => (pixels, false),
                SavedRegionMask::Cropped(pixels) => (pixels, true),
            };
            pixels
                .slice_each_axis(|axis| {
                    let index = axis.axis.index();
                    let offset = if cropped { start[index] - self.start[index] } else { start[index] };
                    Slice::from(offset..offset + shape[index])
                })
                .to_owned()
        });

        match (box_mask, region_mask) {
            (Some(mut bm), Some(pm)) => {
                // `box` and `region` combine by spatial union (CASA fromBCS).
                Zip::from(&mut bm).and(&pm).for_each(|b, &p| *b = *b || p);
                if !bm.iter().all(|&v| v) {
                    and_mask(mask, &bm);
                }
            }
            (Some(bm), None) => and_mask(mask, &bm),
            (None, Some(pm)) => {
                if !pm.iter().all(|&v| v) {
                    and_mask(mask, &pm);
                }
            }
            (None, None) => {}
        }

        if !self.qualified_shapes.is_empty() {
            let qualified = ArrayD::from_shape_fn(IxDyn(shape), |idx| {
                let x = start[px] + idx[px];
                let y = start[py] + idx[py];
                self.qualified_shapes.iter().any(|region| {
                    let contains = |axis: Option<usize>, selected: Option<&[usize]>| {
                        axis.zip(selected).is_none_or(|(axis, selected)| {
                            selected.contains(&(start[axis] + idx[axis]))
                        })
                    };
                    contains(self.channel_axis, region.planes.channels.as_deref())
                        && contains(self.stokes_axis, region.planes.stokes.as_deref())
                        && shape_contains(&region.spatial, x, y)
                })
            });
            if qualified.iter().any(|&value| !value) {
                and_mask(mask, &qualified);
            }
        }

        if let (Some(ax), Some(indices)) = (self.channel_axis, &self.channel_indices)
            && indices.len() != self.shape[ax]
        {
            mask_skipped_axis(mask, shape, ax, start[ax], indices);
        }

        if let (Some(ax), Some(indices)) = (self.stokes_axis, &self.stokes_indices)
            && indices.len() != self.shape[ax]
        {
            mask_skipped_axis(mask, shape, ax, start[ax], indices);
        }
    }
}

fn and_mask(dst: &mut Option<ArrayD<bool>>, rhs: &ArrayD<bool>) {
    *dst = Some(match dst.take() {
        Some(mut lhs) => {
            Zip::from(&mut lhs).and(rhs).for_each(|a, &b| *a = *a && b);
            lhs
        }
        None => rhs.clone(),
    });
}

fn mask_skipped_axis(
    mask: &mut Option<ArrayD<bool>>,
    shape: &[usize],
    axis: usize,
    start: usize,
    kept: &[usize],
) {
    let mut m = mask
        .take()
        .unwrap_or_else(|| ArrayD::from_elem(IxDyn(shape), true));
    for (local, mut slice) in m.axis_iter_mut(Axis(axis)).enumerate() {
        if !kept.contains(&(start + local)) {
            slice.fill(false);
        }
    }
    *mask = Some(m);
}
