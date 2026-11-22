// SPDX-License-Identifier: LGPL-3.0-or-later

mod structure;

use std::path::Path;

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, LinearCoordinate,
};
use casa_lattices::{TraversalCursorIter, TraversalSpec};
use casa_types::{ArrayD, Complex32, Complex64, RecordValue};
use ndarray::{Axis, Ix2, IxDyn};

use crate::{ImageError, ImageInfo, ImageInterface, ImagePixel, ImageType, MutableImageInterface};

use super::super::{axes::StridedSection, lel_mask::PreparedMask};
use super::{SubimageOptions, SubimagePlan, SubimageRegion, read_window::PixelSections};
use structure::StructureFunction;

/// Single precision image pixels accepted by the CASA structure function.
pub trait StructurePixel: ImagePixel + Copy {
    /// Promote a sample for pair accumulation and Fourier transforms.
    fn as_complex64(self) -> Complex64;
    /// Convert the accumulated structure function to its native pixel type.
    fn from_complex64(value: Complex64) -> Self;
    /// Convert a Fourier estimate, snapping negative real roundoff to zero.
    fn from_fft_estimate(value: Complex64) -> Self {
        Self::from_complex64(value)
    }
}

impl StructurePixel for f32 {
    fn as_complex64(self) -> Complex64 {
        Complex64::new(f64::from(self), 0.0)
    }

    fn from_complex64(value: Complex64) -> Self {
        value.re as Self
    }

    fn from_fft_estimate(value: Complex64) -> Self {
        value.re.max(0.0) as Self
    }
}

impl StructurePixel for Complex32 {
    fn as_complex64(self) -> Complex64 {
        Complex64::new(f64::from(self.re), f64::from(self.im))
    }

    fn from_complex64(value: Complex64) -> Self {
        Complex32::new(value.re as f32, value.im as f32)
    }
}

/// Selection and output options for an image's two-point structure function.
pub struct TwoPointOptions<'a> {
    /// Two pixel axes, or an empty list or `[-1]` to choose the direction plane.
    pub axes: &'a [i64],
    /// Method selector. CASA matches names containing the letters `str`.
    pub method: &'a str,
    /// Parent image region resolved before measuring any lags.
    pub region: Option<SubimageRegion<'a>>,
    /// Additional validity expression in the parent image's coordinates.
    pub mask: Option<&'a str>,
    /// Stretch compatible singleton dimensions in the additional mask.
    pub stretch: bool,
    /// Path of the final named output, for protecting mask operands.
    pub destination: Option<&'a Path>,
}

/// Geometry and source selection for a two-point correlation product.
pub struct ImageTwoPoint<'a> {
    /// Selected source section and source beam planes.
    pub source: SubimagePlan,
    /// Two lag axes and all unchanged batch axes.
    pub shape: Vec<usize>,
    /// Direction lag coordinates, if requested, and retained batch coordinates.
    pub coordinates: CoordinateSystem,
    /// The native coordinate record, including the original record layout.
    pub coordinate_record: RecordValue,
    /// Output metadata with the original selected beams and undefined image type.
    pub image_info: ImageInfo,
    axes: [usize; 2],
    options: TwoPointOptions<'a>,
}

impl<'a> ImageTwoPoint<'a> {
    /// Validate the method, select the axes and construct the lag grid.
    ///
    /// # Errors
    /// No supported method or pair of axes was selected, the region is
    /// unreadable, or the lag grid cannot retain the input metadata.
    pub fn new<T: StructurePixel>(
        image: &dyn ImageInterface<T>,
        native: RecordValue,
        beams: &RecordValue,
        options: TwoPointOptions<'a>,
    ) -> Result<Self, ImageError> {
        if !options.method.to_ascii_lowercase().contains("str") {
            return Err(invalid("Unimplemented two-point correlation method"));
        }
        let source = SubimagePlan::new_with_beams(
            image,
            Some(native),
            SubimageOptions {
                region: options.region,
                dropdeg: false,
                keepaxes: &[],
            },
            Some(beams),
        )?;
        source.require_single_region()?;
        let mut coordinates = source.coordinates.clone();
        let sky = coordinates
            .find_coordinate(CoordinateType::Direction)
            .and_then(|index| {
                let axes = coordinates.coordinate_pixel_axes(index)?;
                let [Some(x), Some(y)] = axes.as_slice() else {
                    return None;
                };
                Some((index, [*x, *y]))
            });
        let axes = ordered_axes(options.axes, sky.map(|(_, axes)| axes), source.shape.len())?;
        let mut shape = source.shape.clone();
        let mut image_info = source.image_info().clone();
        for axis in axes {
            let length = shape[axis];
            if length > 1
                && let Some((index, _)) = coordinates.pixel_axis_owner(axis)
            {
                let coord = coordinates.coordinate(index).coordinate_type();
                if coord == CoordinateType::Stokes {
                    return Err(invalid("A Stokes axis cannot be expanded to a lag axis"));
                }
                if coord == CoordinateType::Spectral && image_info.beam_set.n_channels() > 1 {
                    return Err(invalid(
                        "A spectral lag axis cannot keep per-channel restoring beams",
                    ));
                }
            }
            shape[axis] = length
                .checked_add(length - 1)
                .ok_or_else(|| invalid("Two-point lag length overflows"))?;
        }
        let coordinate_record = if let Some((index, sky_axes)) = sky
            && axes.iter().all(|axis| sky_axes.contains(axis))
        {
            let CoordinateModel::Direction(direction) = coordinates.coordinate(index) else {
                unreachable!()
            };
            let names = direction
                .axis_names()
                .into_iter()
                .map(|name| format!("Lag-{name}"))
                .collect();
            let (_, increments) = direction.reference_and_increment_in_world_units();
            let pivot = sky_axes.map(|axis| (source.shape[axis] - 1) as f64);
            let linear = LinearCoordinate::new(2, names, direction.world_units().to_vec())
                .with_increment(increments)
                .with_reference_pixel(pivot.to_vec());
            coordinates.replace_coordinate(index, linear);
            source.transformed_coordinate_record(&coordinates, &sky_axes)?
        } else {
            source
                .coordinate_record
                .clone()
                .unwrap_or_else(|| coordinates.to_record())
        };
        image_info.image_type = ImageType::Undefined;
        Ok(Self {
            source,
            shape,
            coordinates,
            coordinate_record,
            image_info,
            axes,
            options,
        })
    }

    /// Measure the mean squared difference at each lag, one plane at a time.
    /// Both endpoints of a pair must be valid. Empty lags are masked.
    ///
    /// # Errors
    /// Mask resolution, pixel I/O or the Fourier transform failed.
    pub fn copy_to<T: StructurePixel>(
        &self,
        input: &dyn ImageInterface<T>,
        output: &mut dyn MutableImageInterface<T>,
    ) -> Result<(), ImageError> {
        if output.shape() != self.shape {
            return Err(ImageError::ShapeMismatch {
                expected: self.shape.clone(),
                got: output.shape().to_vec(),
            });
        }
        let selection = &self.source.selection;
        let reader = PixelSections {
            image: input,
            selection,
            mask: PreparedMask::new_for_output(
                self.options.mask,
                input.shape(),
                input.coordinates(),
                self.options.stretch,
                self.options.destination,
            )?,
        };
        let ones = vec![1; input.ndim()];
        let section = StridedSection {
            start: &selection.start,
            shape: &self.source.shape,
            stride: selection.stride.as_deref().unwrap_or(&ones),
        };
        output.set_units_string("count".into())?;
        output.set_misc_info(input.misc_info())?;
        output.set_image_info(&self.image_info)?;
        let mask = output.create_filled_pixel_mask(true)?;
        let [x, y] = self.axes;
        let mut input_plane_shape = ones.clone();
        let mut output_plane_shape = ones.clone();
        let input_size = self.axes.map(|axis| self.source.shape[axis]);
        let output_size = self.axes.map(|axis| self.shape[axis]);
        [input_plane_shape[x], input_plane_shape[y]] = input_size;
        [output_plane_shape[x], output_plane_shape[y]] = output_size;
        let mut plan = StructureFunction::new(input_size, output_size)?;
        let cursors = TraversalCursorIter::new(
            section.shape.to_vec(),
            input_plane_shape.clone(),
            TraversalSpec::tiles(),
        );
        for plane in cursors {
            let plane = plane?;
            let start = section.source_start(&plane.position);
            let (pixels, valid) = reader.read(&start, &input_plane_shape, section.stride)?;
            let pixels = collapse_plane(pixels, self.axes);
            let valid = valid.map(|valid| collapse_plane(valid, self.axes));
            let (values, valid) =
                plan.measure(pixels.view(), valid.as_ref().map(|mask| mask.view()))?;
            let values = values
                .into_shape_with_order(IxDyn(&output_plane_shape))
                .expect("lag plane shape");
            output.put_slice(&values, &plane.position)?;
            if valid.iter().any(|bit| !bit) {
                let valid = valid
                    .into_shape_with_order(IxDyn(&output_plane_shape))
                    .expect("lag mask plane shape");
                output.put_mask_slice(&mask, &valid, &plane.position)?;
            }
        }
        Ok(())
    }
}

fn collapse_plane<T>(mut data: ArrayD<T>, axes: [usize; 2]) -> ndarray::Array2<T> {
    for index in (0..data.ndim()).rev() {
        if !axes.contains(&index) {
            data = data.index_axis_move(Axis(index), 0);
        }
    }
    data.into_dimensionality::<Ix2>()
        .expect("selected two image axes")
}

fn ordered_axes(
    axes: &[i64],
    sky: Option<[usize; 2]>,
    rank: usize,
) -> Result<[usize; 2], ImageError> {
    let plane = match axes {
        [] | [-1] => sky.unwrap_or([0, 1]),
        [x, y] => [*x as usize, *y as usize],
        _ => return Err(invalid("The axes argument must be of length 0 or 2")),
    };
    if plane[0] >= rank || plane[1] >= rank || plane[0] >= plane[1] {
        return Err(invalid(
            "Two-point correlation requires two increasing pixel axes",
        ));
    }
    if let Some(sky) = sky
        && sky.contains(&plane[0])
        && !sky.contains(&plane[1])
    {
        return Err(invalid("Cannot select just one direction axis"));
    }
    Ok(plane)
}

fn invalid(message: impl Into<String>) -> ImageError {
    ImageError::CasaSelection(message.into())
}
