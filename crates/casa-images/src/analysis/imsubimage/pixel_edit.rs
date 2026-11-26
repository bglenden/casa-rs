// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::ArrayD;
use ndarray::{IxDyn, Zip};

use crate::{ImageError, MutableImageInterface};

use super::{SubimageRegion, region::SubimageSelection};

/// The region selected for a constant pixel or mask edit.
pub struct ImagePixelSelection(SubimageSelection);

impl ImagePixelSelection {
    /// Resolve the edit region against the attached image.
    pub fn new(
        image: &dyn crate::ImageInterface<f32>,
        region: Option<SubimageRegion<'_>>,
    ) -> Result<Self, ImageError> {
        SubimageSelection::resolve(image, None, region, None, None).map(Self)
    }

    /// Edit only selected pixels. Return the origin and extent for reporting.
    pub fn apply(
        self,
        image: &mut dyn MutableImageInterface<f32>,
        value: Option<f32>,
        pixel_mask: Option<(&str, bool)>,
    ) -> Result<(Vec<usize>, Vec<usize>), ImageError> {
        let selection = self.0;
        let mut membership = None;
        selection.apply_selection_masks(&mut membership);
        let stride = vec![1; image.ndim()];

        let pixels = value
            .map(|value| {
                patch_selected(&selection.shape, membership.as_ref(), value, || {
                    image
                        .get_slice(&selection.start, &selection.shape, &stride)
                        .map_err(Into::into)
                })
            })
            .transpose()?;
        let edited_mask = pixel_mask
            .map(|(name, value)| {
                patch_selected(&selection.shape, membership.as_ref(), value, || {
                    image.get_named_mask_slice(name, &selection.start, &selection.shape, &stride)
                })
                .map(|mask| (name, mask))
            })
            .transpose()?;

        if let Some(pixels) = pixels {
            image.put_slice(&pixels, &selection.start)?;
        }
        if let Some((name, mask)) = edited_mask {
            image.put_mask_slice(name, &mask, &selection.start)?;
        }
        Ok((selection.start, selection.shape))
    }
}

fn patch_selected<T: Copy>(
    shape: &[usize],
    mask: Option<&ArrayD<bool>>,
    value: T,
    previous: impl FnOnce() -> Result<ArrayD<T>, ImageError>,
) -> Result<ArrayD<T>, ImageError> {
    let Some(mask) = mask else {
        return Ok(ArrayD::from_elem(IxDyn(shape), value));
    };
    let mut pixels = previous()?;
    Zip::from(&mut pixels).and(mask).for_each(|pixel, &valid| {
        if valid {
            *pixel = value;
        }
    });
    Ok(pixels)
}
