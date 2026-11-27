// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::ArrayD;
use casa_lattices::{TraversalCursorIter, TraversalSpec};
use ndarray::{Array1, IxDyn};

use casa_images::{ImageError, ImageInterface, MutableImageInterface};

use super::PROFILE_TILE_SAMPLES;
use super::kernel::{LineConvolution, Weights};

pub(super) fn convolve(
    image: &mut dyn MutableImageInterface<f32>,
    axis: usize,
    weights: &Weights,
    [past, future]: [usize; 2],
    stride: &[usize],
) -> Result<(), ImageError> {
    let mut profiles = image.shape().to_vec();
    let length = profiles[axis];
    profiles[axis] = 1;
    let step = PROFILE_TILE_SAMPLES - past - future;
    let mut filter = LineConvolution::new(PROFILE_TILE_SAMPLES, weights)?;
    let mut window = Array1::<f32>::zeros(PROFILE_TILE_SAMPLES);
    let mut carry = vec![0.0; past];
    let cursors = TraversalCursorIter::new(profiles, stride.to_vec(), TraversalSpec::tiles());
    for profile in cursors {
        let origin = profile?.position;
        let section = Profile {
            origin,
            axis,
            length,
            stride,
        };
        let finite = section.is_finite(image, step)?;
        carry.fill(0.0);
        for first in (0..length).step_by(step) {
            let count = step.min(length - first);
            let position = section.position(first);
            let shape = section.shape(count);
            let pixels = if finite {
                let raw = section.read(image, first, (count + future).min(length - first))?;
                let data = window.as_slice_mut().expect("contiguous profile window");
                data[..past].copy_from_slice(&carry);
                for (index, &value) in raw.iter().enumerate() {
                    data[past + index] = value;
                }
                data[past + raw.len()..].fill(0.0);
                // Keep the original tail across an in-place write. The next
                // window still needs these samples for its left edge.
                carry.copy_from_slice(&data[count..count + past]);
                filter.apply(window.view_mut())?;
                let data = window.as_slice().expect("contiguous profile window");
                ArrayD::from_shape_vec(IxDyn(&shape), data[past..past + count].to_vec())
                    .expect("one profile block")
            } else {
                ArrayD::from_elem(IxDyn(&shape), f32::NAN)
            };
            image.put_slice(&pixels, &position)?;
        }
    }
    Ok(())
}

struct Profile<'a> {
    origin: Vec<usize>,
    axis: usize,
    length: usize,
    stride: &'a [usize],
}

impl Profile<'_> {
    fn position(&self, pixel: usize) -> Vec<usize> {
        let mut position = self.origin.clone();
        position[self.axis] = pixel;
        position
    }

    fn shape(&self, count: usize) -> Vec<usize> {
        let mut shape = self.stride.to_vec();
        shape[self.axis] = count;
        shape
    }

    fn read(
        &self,
        image: &dyn ImageInterface<f32>,
        first: usize,
        count: usize,
    ) -> Result<ArrayD<f32>, ImageError> {
        Ok(image.get_slice(&self.position(first), &self.shape(count), self.stride)?)
    }

    fn is_finite(&self, image: &dyn ImageInterface<f32>, step: usize) -> Result<bool, ImageError> {
        for first in (0..self.length).step_by(step) {
            let values = self.read(image, first, step.min(self.length - first))?;
            if values.iter().any(|value| !value.is_finite()) {
                return Ok(false);
            }
        }
        Ok(true)
    }
}
