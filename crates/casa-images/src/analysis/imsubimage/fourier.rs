// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_coordinates::{CoordinateSystem, CoordinateType};
use casa_fft::{Fft2, FftScalar};
use casa_lattices::{ScratchSpace, TraversalCursorIter, TraversalSpec};
use casa_types::RecordValue;
use ndarray::{Axis, Zip};
use num_complex::Complex;

use crate::image_expr::bounded_read_cursor;
use crate::{
    ImageError, ImageInterface, ImagePixel, MutableImageInterface, TempImage, TempStoragePolicy,
};

use super::super::lel_mask::PreparedMask;
use super::read_window::PixelSections;
use super::{StridedSection, SubimageOptions, SubimagePlan, SubimageRegion};

/// A native real or complex pixel and the transform precision it uses.
pub trait FourierPixel: ImagePixel {
    /// Real precision of the transform.
    type Scalar: ImagePixel + FftScalar;

    /// Represent one pixel in the transform's complex precision.
    fn complex(self) -> Complex<Self::Scalar>;

    /// Return an amplitude in the same native precision.
    fn amplitude(value: Complex<Self::Scalar>) -> Self::Scalar;

    /// Return a phase in radians in the same native precision.
    fn phase(value: Complex<Self::Scalar>) -> Self::Scalar;
}

macro_rules! fourier_pixels {
    ($scalar:ty) => {
        impl FourierPixel for $scalar {
            type Scalar = $scalar;

            fn complex(self) -> Complex<Self::Scalar> {
                Complex::new(self, 0.)
            }

            fn amplitude(value: Complex<Self::Scalar>) -> Self::Scalar {
                value.norm()
            }

            fn phase(value: Complex<Self::Scalar>) -> Self::Scalar {
                value.arg()
            }
        }

        impl FourierPixel for Complex<$scalar> {
            type Scalar = $scalar;

            fn complex(self) -> Complex<Self::Scalar> {
                self
            }

            fn amplitude(value: Complex<Self::Scalar>) -> Self::Scalar {
                value.norm()
            }

            fn phase(value: Complex<Self::Scalar>) -> Self::Scalar {
                value.arg()
            }
        }
    };
}

fourier_pixels!(f32);
fourier_pixels!(f64);

/// Pixel selection and transform axes for an image Fourier transform.
pub struct FourierOptions<'a> {
    /// Optional image region.
    pub region: Option<SubimageRegion<'a>>,
    /// An empty list or a single negative value selects both sky axes.
    pub axes: &'a [i64],
    /// Additional validity expression on the source.
    pub mask: Option<&'a str>,
    /// Broadcast a compatible lower-rank mask.
    pub stretch: bool,
    /// Published destinations, checked against each named mask operand.
    pub destinations: &'a [&'a Path],
}

/// Image selection and transformed coordinates for the FFT.
pub struct ImageFourier<'a> {
    section: SubimagePlan,
    axes: Vec<usize>,
    coordinates: CoordinateSystem,
    coordinate_record: RecordValue,
    options: FourierOptions<'a>,
}

fn invalid(message: impl Into<String>) -> ImageError {
    ImageError::CasaSelection(message.into())
}

impl<'a> ImageFourier<'a> {
    /// Resolve the selected image domain and Fourier coordinates.
    ///
    /// # Errors
    /// Returns an error for an unsupported selection, invalid axes, or an
    /// invalid preserved coordinate model.
    pub fn new<T: FourierPixel>(
        image: &dyn ImageInterface<T>,
        coordinate_record: RecordValue,
        beam_record: &RecordValue,
        options: FourierOptions<'a>,
    ) -> Result<Self, ImageError> {
        let section = SubimagePlan::new_with_beams(
            image,
            Some(coordinate_record),
            SubimageOptions {
                region: options.region,
                dropdeg: false,
                keepaxes: &[],
            },
            Some(beam_record),
        )?;
        section.require_single_region()?;
        let mut axes = if options.axes.is_empty() || matches!(options.axes, [axis] if *axis < 0) {
            section
                .coordinates
                .find_coordinate(CoordinateType::Direction)
                .and_then(|index| section.coordinates.coordinate_pixel_axes(index))
                .filter(|axes| axes.len() == 2 && axes.iter().all(Option::is_some))
                .ok_or_else(|| invalid("Coordinate system does not have a direction coordinate"))?
                .into_iter()
                .flatten()
                .collect()
        } else {
            options
                .axes
                .iter()
                .map(|&axis| {
                    let axis = usize::try_from(axis).map_err(|_| {
                        invalid("None of the elements of axes may be less than zero")
                    })?;
                    if axis >= section.shape.len() {
                        return Err(invalid(format!(
                            "Fourier axis {axis} is outside an image of rank {}",
                            section.shape.len()
                        )));
                    }
                    Ok(axis)
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        axes.sort_unstable();
        axes.dedup();
        let coordinates = section.coordinates.fourier_axes(&section.shape, &axes)?;
        let beams = &section.image_info().beam_set;
        if beams.is_multi() {
            let planes = |kind| {
                coordinates
                    .find_coordinate(kind)
                    .and_then(|index| coordinates.coordinate_pixel_axes(index))
                    .and_then(|axes| axes.into_iter().flatten().next())
                    .map_or(1, |axis| section.shape[axis])
            };
            if beams.n_channels() != 1 && beams.n_channels() != planes(CoordinateType::Spectral) {
                return Err(invalid("Number of channels is not consistent"));
            }
            if beams.n_stokes() != 1 && beams.n_stokes() != planes(CoordinateType::Stokes) {
                return Err(invalid("Number of polarizations is not consistent"));
            }
            if (0..beams.n_channels()).any(|channel| {
                (0..beams.n_stokes()).any(|stokes| beams.beam(channel, stokes).is_null())
            }) {
                return Err(invalid(
                    "At least one of the beams in the beam set is null and thus invalid",
                ));
            }
        }
        let coordinate_record = section.transformed_coordinate_record(&coordinates, &axes)?;
        Ok(Self {
            section,
            axes,
            coordinates,
            coordinate_record,
            options,
        })
    }

    /// The shape retained from the selected image.
    pub fn shape(&self) -> &[usize] {
        &self.section.shape
    }

    /// Source selection, including retained beam planes and image metadata.
    pub fn section(&self) -> &SubimagePlan {
        &self.section
    }

    /// Fourier coordinates for an allocated output.
    pub fn coordinates(&self) -> &CoordinateSystem {
        &self.coordinates
    }

    /// Native Fourier coordinate record, retaining untouched coordinate fields.
    pub fn coordinate_record(&self) -> &RecordValue {
        &self.coordinate_record
    }

    /// Transform the selected region to complex scratch storage.
    ///
    /// # Errors
    /// The mask is incompatible or overlaps an output, or FFT planning,
    /// allocation, or image I/O fails.
    pub fn transform<T: FourierPixel>(
        &self,
        image: &dyn ImageInterface<T>,
    ) -> Result<TempImage<Complex<T::Scalar>>, ImageError>
    where
        Complex<T::Scalar>: ImagePixel,
    {
        let selection = &self.section.selection;
        let reader = PixelSections {
            image,
            selection,
            mask: PreparedMask::new_for_outputs(
                self.options.mask,
                image.shape(),
                image.coordinates(),
                self.options.stretch,
                self.options.destinations.iter().copied(),
            )?,
        };
        let unit = vec![1; self.shape().len()];
        let selected = StridedSection {
            start: &selection.start,
            shape: self.shape(),
            stride: selection.stride.as_deref().unwrap_or(&unit),
        };
        let mut scratch = TempImage::<Complex<T::Scalar>>::new(
            self.shape().to_vec(),
            self.coordinates.clone(),
            TempStoragePolicy::Auto {
                memory_budget_bytes: 16 * 1024 * 1024,
                scratch: ScratchSpace::SystemTemp,
            },
        )?;
        scratch.set_pixel_cache_budget(16 * 1024 * 1024)?;
        for tile in selected.cursors(bounded_read_cursor(selected.dense_shape(selected.shape))?) {
            let tile = tile?;
            let (pixels, validity) = reader.read(
                &selected.source_start(&tile.position),
                &tile.shape,
                selected.stride,
            )?;
            let mut pixels = pixels.mapv(T::complex);
            if let Some(mask) = validity {
                set_mask(&mut scratch, &mask, &tile.position)?;
                Zip::from(&mut pixels).and(&mask).for_each(|value, &valid| {
                    if !valid {
                        *value = Complex::default();
                    }
                });
            }
            scratch.put_slice(&pixels, &tile.position)?;
        }
        for &axis in &self.axes {
            self.transform_axis::<T::Scalar>(&mut scratch, axis)?;
        }
        Ok(scratch)
    }

    fn transform_axis<T: FftScalar>(
        &self,
        scratch: &mut TempImage<Complex<T>>,
        axis: usize,
    ) -> Result<(), ImageError>
    where
        Complex<T>: ImagePixel,
    {
        let length = self.shape()[axis];
        if length == 1 {
            return Ok(());
        }
        let fft_error = |error: casa_fft::FftError| invalid(error.to_string());
        let mut fft = Fft2::<T>::with_threads([length, 1], 1)
            .map_err(fft_error)?
            .with_estimated_plan();
        let mut lane_buffer = vec![Complex::default(); length];
        let mut cursor = vec![1; self.shape().len()];
        cursor[axis] = length;
        let mut remaining = (128 * 1024 / length).max(1);
        for (i, &extent) in self.shape().iter().enumerate() {
            if i != axis {
                cursor[i] = extent.min(remaining);
                remaining /= cursor[i];
            }
        }
        for tile in TraversalCursorIter::new(self.shape().to_vec(), cursor, TraversalSpec::tiles())
        {
            let tile = tile?;
            let mut pixels = scratch.get_slice(&tile.position, &tile.shape)?;
            for mut lane in pixels.lanes_mut(Axis(axis)) {
                for (index, entry) in lane_buffer.iter_mut().enumerate() {
                    *entry = lane[(index + length / 2) % length];
                }
                fft.transform(&mut lane_buffer, false).map_err(fft_error)?;
                for (index, part) in lane.iter_mut().enumerate() {
                    *part = lane_buffer[(index + length.div_ceil(2)) % length];
                }
            }
            scratch.put_slice(&pixels, &tile.position)?;
        }
        Ok(())
    }

    /// Copy one product's values, mask, and metadata from the shared transform.
    ///
    /// # Errors
    /// The destination shape is wrong or image I/O fails.
    pub fn write_product<S: ImagePixel, C: ImagePixel, D: ImagePixel>(
        &self,
        source: &dyn ImageInterface<S>,
        scratch: &dyn ImageInterface<C>,
        output: &mut dyn MutableImageInterface<D>,
        phase: bool,
        component: impl Fn(C) -> D,
    ) -> Result<(), ImageError> {
        self.section.copy_metadata(source, output)?;
        let unit = if phase {
            "rad"
        } else {
            match source.units() {
                "Jy/beam" | "Jy/pixel" => "Jy",
                "Jy" if self.section.image_info().beam_set.is_empty() => "Jy/pixel",
                "Jy" => "Jy/beam",
                other => other,
            }
        };
        output.set_units_string(unit.to_owned())?;
        let cursor = bounded_read_cursor(self.shape().to_vec())?;
        let unit_stride = vec![1; self.shape().len()];
        for tile in TraversalCursorIter::new(self.shape().to_vec(), cursor, TraversalSpec::tiles())
        {
            let tile = tile?;
            let values = scratch.get_slice(&tile.position, &tile.shape, &unit_stride)?;
            output.put_slice(&values.mapv(&component), &tile.position)?;
            if let Some(mask) = scratch.get_mask_slice(&tile.position, &tile.shape, &unit_stride)? {
                set_mask(output, &mask, &tile.position)?;
            }
        }
        Ok(())
    }
}

fn set_mask<T: ImagePixel>(
    image: &mut dyn MutableImageInterface<T>,
    mask: &casa_types::ArrayD<bool>,
    start: &[usize],
) -> Result<(), ImageError> {
    if image.default_mask_name().is_none() {
        image.create_filled_mask("mask0", true)?;
        image.set_default_mask("mask0")?;
    }
    image.put_mask_slice("mask0", mask, start)
}
