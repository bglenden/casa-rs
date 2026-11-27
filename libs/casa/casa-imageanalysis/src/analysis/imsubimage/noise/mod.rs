// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::ArrayD;
use casa_lattices::{TraversalCursorIter, TraversalSpec};
use ndarray::{IxDyn, SliceInfoElem};

use casa_images::{ImageError, ImagePixel, MutableImageInterface};

use super::{SubimageRegion, region::StridedSection};

mod random;
use random::{Distribution, Generator};

/// The casacore noise distribution and its per-call random sequence.
pub struct ImageNoise {
    distribution: Distribution,
    generator: Generator,
}

impl ImageNoise {
    /// Validate the distribution before making any changes to the image.
    ///
    /// Two seeds determine the sequence. A missing seed comes from the clock.
    /// # Errors
    /// The distribution name or parameters are invalid.
    pub fn new(kind: &str, parameters: &[f64], seeds: &[i64]) -> Result<Self, ImageError> {
        Ok(Self {
            distribution: Distribution::parse(kind, parameters)?,
            generator: Generator::new(seeds),
        })
    }

    /// Draw one real value from this distribution.
    pub fn draw(&mut self) -> f64 {
        self.distribution.draw(&mut self.generator)
    }

    /// Add noise over the region's lattice section in CASA cursor order.
    ///
    /// The existing mask stays in place. Noise is added to masked pixels
    /// within the bounding box, as with casacore's LatticeAddNoise.
    /// # Errors
    /// The region cannot be resolved or a pixel access fails.
    pub fn apply<T: ImagePixel>(
        &mut self,
        image: &mut dyn MutableImageInterface<T>,
        region: Option<SubimageRegion<'_>>,
        zero: bool,
        add_sample: impl Fn(T, &mut ImageNoise) -> T,
    ) -> Result<(), ImageError> {
        let selection = StridedSection::resolve(image, region)?;
        let rank = selection.count.len();
        let ones = vec![1; rank];
        let cursor_shape = image.nice_cursor_shape();
        let unit_stride = selection.stride == ones;
        let cursors =
            TraversalCursorIter::new(selection.count, cursor_shape, TraversalSpec::tiles());
        for cursor in cursors {
            let cursor = cursor?;
            let origin: Vec<_> = selection
                .start
                .iter()
                .zip(&cursor.position)
                .zip(&selection.stride)
                .map(|((&start, &position), &step)| start + position * step)
                .collect();
            let extent: Vec<_> = cursor
                .shape
                .iter()
                .zip(&selection.stride)
                .map(|(&count, &stride)| (count - 1) * stride + 1)
                .collect();
            let bytes = extent
                .iter()
                .fold(T::ELEM_SIZE, |total, &length| total.saturating_mul(length));
            if !unit_stride && bytes > 8 * 1024 * 1024 {
                // A sparse section can cover many tiles. Visit its samples in
                // the same order without allocating the full bounding window.
                let points =
                    TraversalCursorIter::new(cursor.shape, ones.clone(), TraversalSpec::tiles());
                for point in points {
                    let point = point?;
                    let position: Vec<_> = origin
                        .iter()
                        .zip(&point.position)
                        .zip(&selection.stride)
                        .map(|((&start, &position), &step)| start + position * step)
                        .collect();
                    let existing = if zero {
                        T::default_value()
                    } else {
                        image.get_at(&position)?
                    };
                    image.put_at(add_sample(existing, self), &position)?;
                }
                continue;
            }
            let mut pixels = if zero && unit_stride {
                ArrayD::from_elem(IxDyn(&extent), T::default_value())
            } else {
                image.get_slice(&origin, &extent, &ones)?
            };
            let slicing: Vec<_> = selection
                .stride
                .iter()
                .map(|&stride| SliceInfoElem::Slice {
                    start: 0,
                    end: None,
                    step: stride as isize,
                })
                .collect();
            let selected = pixels.slice_mut(slicing.as_slice()).reversed_axes();
            for value in selected {
                let previous = if zero { T::default_value() } else { *value };
                *value = add_sample(previous, self);
            }
            image.put_slice(&pixels, &origin)?;
        }
        Ok(())
    }
}
