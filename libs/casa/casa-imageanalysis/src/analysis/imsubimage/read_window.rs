// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::ArrayD;
use ndarray::Slice;

use casa_images::{ImageError, ImageInterface, ImagePixel};

use super::super::axes::StridedSection;
use super::super::lel_mask::PreparedMask;
use super::and_opt_mask;
use super::region::SubimageSelection;

pub(in crate::analysis) struct PixelSections<'a, T: ImagePixel> {
    pub(in crate::analysis) image: &'a dyn ImageInterface<T>,
    pub(in crate::analysis) selection: &'a SubimageSelection,
    pub(in crate::analysis) mask: Option<PreparedMask<'a>>,
}

impl<T: ImagePixel> PixelSections<'_, T> {
    pub(in crate::analysis) fn read(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<(ArrayD<T>, Option<ArrayD<bool>>), ImageError> {
        let pixels = self.image.get_slice(start, shape, stride)?;
        let mut validity = self.image.get_mask_slice(start, shape, stride)?;
        let section = StridedSection {
            start,
            shape,
            stride,
        };
        if self.selection.stride.is_none() {
            let extent = section.dense_shape(shape);
            let mut membership = None;
            self.selection
                .apply_window_masks(&mut membership, start, &extent);
            if let Some(membership) = membership {
                let membership = membership.slice_each_axis(|slice| {
                    let axis = slice.axis.index();
                    sample_slice(0, shape[axis], stride[axis])
                });
                and_opt_mask(&mut validity, membership.to_owned());
            }
        }
        if let Some(mask) = &self.mask {
            and_opt_mask(&mut validity, mask.strided(&section)?);
        }
        Ok((pixels, validity))
    }
}

pub(super) fn sample_slice(start: usize, count: usize, step: usize) -> Slice {
    Slice::new(
        start as isize,
        Some((start + (count - 1) * step + 1) as isize),
        step as isize,
    )
}
