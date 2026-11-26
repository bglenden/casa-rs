// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::ArrayD;
use ndarray::{Dimension, IxDyn};

use casa_images::{ImageError, ImageInterface, MutableImageInterface};

use super::super::super::axes::StridedSection;
use super::super::{SubimageOptions, SubimagePlan};
use super::{ComponentFitPixel, ImageComponentFit, SkyFit, fit_value};
use casa_images::bounded_read_cursor;

impl ImageComponentFit {
    /// Write requested model and residual products over the original image grid.
    ///
    /// Only fitted planes receive nonzero results. Output masks describe the
    /// source pixels, independently of the fitting region or expression mask.
    ///
    /// # Errors
    /// A product allocation, sample read, mask update or pixel write failed.
    pub fn write_images<'a, T: ComponentFitPixel>(
        &self,
        source: &dyn ImageInterface<T>,
        mut model: Option<&'a mut dyn MutableImageInterface<f32>>,
        mut residual: Option<&'a mut dyn MutableImageInterface<f32>>,
    ) -> Result<(), ImageError> {
        if model.is_none() && residual.is_none() {
            return Ok(());
        }
        let original = SubimagePlan::new(
            source,
            None,
            SubimageOptions {
                region: None,
                dropdeg: false,
                keepaxes: &[],
            },
        )?;
        let initial_validity = source.default_mask_name().is_some();
        let mut mask_names = [None, None];
        for (output, mask) in [&mut model, &mut residual].into_iter().zip(&mut mask_names) {
            if let Some(output) = output {
                original.copy_metadata(source, *output)?;
                if initial_validity {
                    *mask = Some(output.create_filled_pixel_mask(true)?);
                }
            }
        }
        let start = vec![0; source.ndim()];
        let stride = vec![1; source.ndim()];
        let section = StridedSection {
            start: &start,
            shape: source.shape(),
            stride: &stride,
        };
        for tile in section.cursors(bounded_read_cursor(source.shape().to_vec())?) {
            let tile = tile?;
            let position = tile.position;
            let shape = tile.shape;
            let valid = source.get_mask_slice(&position, &shape, &stride)?;
            let mut pixels = ArrayD::zeros(IxDyn(&shape));
            let mut remainder = if residual.is_some() {
                source
                    .get_slice(&position, &shape, &stride)?
                    .mapv(T::fitting_value)
            } else {
                ArrayD::from_shape_vec(IxDyn(&[0]), Vec::new()).expect("empty residual")
            };
            let mut full_pixel = position.clone();
            let write_residual = residual.is_some();
            for (index, output) in pixels.indexed_iter_mut() {
                for ((absolute, &start), &relative) in
                    full_pixel.iter_mut().zip(&position).zip(index.slice())
                {
                    *absolute = start + relative;
                }
                if let Some(fit) = self.model_at(&full_pixel) {
                    let xy = self.selected_xy(&full_pixel);
                    *output = fit_value(fit, xy) as f32;
                    if write_residual {
                        remainder[index] -= *output;
                    }
                } else if write_residual {
                    remainder[index] = 0.;
                }
            }
            for ((output, mask), slice) in [&mut model, &mut residual]
                .into_iter()
                .zip(&mut mask_names)
                .zip([&pixels, &remainder])
            {
                if let Some(output) = output {
                    output.put_slice(slice, &position)?;
                    if let Some(valid) = &valid {
                        if mask.is_none() && valid.iter().any(|&value| !value) {
                            *mask = Some(output.create_filled_pixel_mask(true)?);
                        }
                        if let Some(mask) = mask.as_deref() {
                            output.put_mask_slice(mask, valid, &position)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn model_at(&self, pixel: &[usize]) -> Option<&SkyFit> {
        let selection = &self.selection;
        if selection
            .region
            .stokes_axis
            .is_some_and(|axis| pixel[axis] != selection.stokes_pixel)
        {
            return None;
        }
        if selection
            .ancillary
            .iter()
            .any(|&(axis, index)| pixel[axis] != index)
        {
            return None;
        }
        let channel = selection.region.channel_axis.map_or(0, |axis| pixel[axis]);
        let index = self
            .planes
            .binary_search_by_key(&channel, |plane| plane.channel)
            .ok()?;
        self.planes[index].model.as_ref()
    }

    fn selected_xy(&self, pixel: &[usize]) -> [f64; 2] {
        std::array::from_fn(|i| {
            let axis = self.selection.sky_axes[i];
            (pixel[axis] as f64 - self.selection.region.start[axis] as f64)
                / self.selection.sky_stride[i] as f64
        })
    }
}
