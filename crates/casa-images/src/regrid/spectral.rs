// SPDX-License-Identifier: LGPL-3.0-or-later
//! The spectral pass of a regrid: casacore's `ImageRegrid::regrid1D` with
//! `Interpolate2D::LINEAR`, as `SIImageStore::regridToModelImage` regrids a
//! start model's spectral axis.
//!
//! Each output channel is converted to world frequency on the template and
//! back to a pixel on the input (`make1DCoordinateGrid`), and every line of
//! the image along the spectral axis is interpolated there by casacore's
//! `InterpolateArray1D::linear` with flags, good being true and without
//! extrapolation, in single precision as casacore runs it for a `Float`
//! image:
//! - between two input channels, their linear interpolation, good where
//!   both are;
//! - within single-precision rounding of an input channel, that channel;
//! - beyond either end, the end channel's value, masked.
//!
//! The pixel values beyond the ends are therefore the end channels' rather
//! than zero, as casacore writes them into an image without a mask (a
//! tclean model): only the mask says they were not sampled.

use casa_coordinates::{CoordinateModel, CoordinateType};
use ndarray::{ArrayD, Axis, IxDyn, ShapeBuilder, Zip};

use crate::ImagePixel;

/// How the spectral axis is regridded: its pixel axis and the input pixel
/// at each output channel, `None` where the conversion fails.
pub(super) struct SpectralPlan {
    axis: usize,
    positions: Vec<Option<f32>>,
}

impl SpectralPlan {
    /// The plan for spectral coordinates `input` of `input_channels` and
    /// `output` of `output_channels` on pixel axis `axis`.
    ///
    /// # Errors
    ///
    /// The reason when the two are not both spectral, are in different
    /// frames (casa-rs makes no frequency conversion machine), the input
    /// holds one channel, which casacore's linear interpolation reads past,
    /// or the output does and the input more: `ImageRegrid::_checkAxes`
    /// removes an axis of unit length from those it regrids and then refuses
    /// the shapes.
    pub(super) fn new(
        (input, input_channels): (&CoordinateModel, usize),
        (output, output_channels): (&CoordinateModel, usize),
        axis: usize,
    ) -> Result<Self, String> {
        let (CoordinateModel::Spectral(ours), CoordinateModel::Spectral(theirs)) = (input, output)
        else {
            return Err(format!(
                "the {} axes differ between the input and the template; casa-rs regrids the \
                 direction and spectral axes only",
                input.coordinate_type()
            ));
        };
        if ours.world_frequency_ref() != theirs.world_frequency_ref() {
            return Err(format!(
                "the input's frequencies are {:?} and the template's {:?}; casa-rs does not \
                 convert between spectral frames",
                ours.world_frequency_ref(),
                theirs.world_frequency_ref()
            ));
        }
        if input_channels < 2 {
            return Err(format!(
                "the input has {input_channels} channel and the template {output_channels}; a \
                 spectral regrid interpolates between two channels"
            ));
        }
        if output_channels == 1 {
            return Err(format!(
                "any axis not being regridded must have the same input and output shapes: the \
                 template's spectral axis has one channel and the input's {input_channels}"
            ));
        }
        debug_assert_eq!(input.coordinate_type(), CoordinateType::Spectral);
        let positions = (0..output_channels)
            .map(|channel| {
                theirs
                    .world_frequency_hz(channel as f64)
                    .and_then(|frequency| ours.pixel_at_frequency_hz(frequency))
                    .ok()
                    .filter(|pixel| pixel.is_finite())
                    .map(|pixel| pixel as f32)
            })
            .collect();
        Ok(Self { axis, positions })
    }

    /// `data` and its `mask` (true where good) regridded along the spectral
    /// axis onto `shape`, which differs from theirs on that axis alone.
    pub(super) fn apply<T>(
        &self,
        data: &ArrayD<T>,
        mask: &ArrayD<bool>,
        shape: &[usize],
    ) -> (ArrayD<T>, ArrayD<bool>)
    where
        T: ImagePixel + Into<f64> + From<f32> + Copy,
    {
        let mut out_data = ArrayD::<T>::from_elem(IxDyn(shape).f(), T::from(0.0));
        let mut out_mask = ArrayD::<bool>::from_elem(IxDyn(shape).f(), false);
        let axis = Axis(self.axis);
        Zip::from(out_data.lanes_mut(axis))
            .and(out_mask.lanes_mut(axis))
            .and(data.lanes(axis))
            .and(mask.lanes(axis))
            .for_each(|mut out, mut good, line, line_mask| {
                for (channel, position) in self.positions.iter().enumerate() {
                    let Some(position) = position else {
                        continue;
                    };
                    let (value, valid) = linear(
                        *position,
                        |k| (Into::<f64>::into(line[k]) as f32, line_mask[k]),
                        line.len(),
                    );
                    out[channel] = T::from(value);
                    good[channel] = valid;
                }
            });
        (out_data, out_mask)
    }
}

/// casacore `InterpolateArray1D::interpolatePtr`'s `linear` case with
/// flags, `goodIsTrue` and no extrapolation, over input pixels `0..count`
/// (`count` at least two): the value at `x` and whether it is good.
fn linear(x: f32, at: impl Fn(usize) -> (f32, bool), count: usize) -> (f32, bool) {
    // `binarySearchBrackets` over 0, 1, …: the input at `x`, else the
    // first beyond it.
    let bracket = if x <= 0.0 {
        0
    } else {
        (x.ceil() as usize).min(count)
    };
    let (right, discard) = match bracket {
        0 => (1, x != 0.0),
        n if n == count => (count - 1, true),
        n => (n, false),
    };
    let left = right - 1;
    let ((y1, good1), (y2, good2)) = (at(left), at(right));
    let frac = (x - left as f32) / ((right - left) as f32);
    let limit = f32::EPSILON;
    if frac > limit && frac < 1.0 - limit {
        (y1 + frac * (y2 - y1), !discard && good1 && good2)
    } else if frac <= limit {
        (y1, !discard && good1)
    } else {
        (y2, !discard && good2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::regrid::{RegridMethod, RegridTemplate, regrid, regrid_along_frequency};

    #[test]
    fn spectral_plan_converts_between_axis_units() {
        use casa_coordinates::SpectralCoordinate;
        use casa_types::measures::frequency::FrequencyRef;

        let input = CoordinateModel::Spectral(Box::new(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            1.0e9,
            1.0e8,
            0.0,
            1.4e9,
        )));
        let output = CoordinateModel::Spectral(Box::new(
            SpectralCoordinate::new(FrequencyRef::LSRK, 1.05, 0.1, 0.0, 1.4).with_unit("GHz"),
        ));
        let plan = SpectralPlan::new((&input, 4), (&output, 3), 0).unwrap();
        for (actual, expected) in plan.positions.iter().zip([0.5_f32, 1.5, 2.5]) {
            assert!((actual.unwrap() - expected).abs() < 1e-5);
        }
    }

    /// A 4 × 4 image of one Stokes plane and `channels` channels from
    /// `first_hz`, `step_hz` apart, each plane holding `value(channel)`.
    fn cube(
        first_hz: f64,
        step_hz: f64,
        channels: usize,
        frame: casa_types::measures::frequency::FrequencyRef,
        value: impl Fn(usize) -> f32,
    ) -> crate::TempImage<f32> {
        use casa_coordinates::{
            CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
            StokesCoordinate, StokesType,
        };
        use casa_types::measures::direction::DirectionRef;
        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [1.0, 0.3],
            [-1e-5, 1e-5],
            [2.0, 2.0],
        ));
        coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
        coordinates.add_coordinate(SpectralCoordinate::new(
            frame, first_hz, step_hz, 0.0, 1.0e9,
        ));
        let shape = vec![4, 4, 1, channels];
        let mut image = crate::TempImage::<f32>::new(
            shape.clone(),
            coordinates,
            casa_lattices::TempStoragePolicy::Memory,
        )
        .unwrap();
        image.set_units("Jy/pixel").unwrap();
        let data = ArrayD::from_shape_fn(IxDyn(&shape).f(), |index| value(index[3]));
        image.put_slice(&data, &[0, 0, 0, 0]).unwrap();
        image
    }

    /// `SIImageStore::regridToModelImage`'s spectral regrid: an output
    /// channel between two input channels takes their line, and one beyond
    /// the input's ends takes the end channel's value, masked.
    #[test]
    fn a_cube_is_regridded_along_frequency_as_casacore_regrids_it() {
        use casa_types::measures::frequency::FrequencyRef;
        let input = cube(1.0e9, 1.0e8, 4, FrequencyRef::LSRK, |k| {
            10.0 * (k as f32 + 1.0)
        });
        let template = RegridTemplate::of(&cube(0.95e9, 1.0e8, 6, FrequencyRef::LSRK, |_| 0.0));
        let regridded = regrid_along_frequency(&input, &template, RegridMethod::Linear).unwrap();
        assert_eq!(regridded.data.shape(), &[4, 4, 1, 6]);
        let planes = (0..6)
            .map(|k| (regridded.data[[1, 2, 0, k]], regridded.mask[[1, 2, 0, k]]))
            .collect::<Vec<_>>();
        assert_eq!(
            planes,
            [
                (10.0, false),
                (15.0, true),
                (25.0, true),
                (35.0, true),
                (40.0, false),
                (40.0, false)
            ]
        );
        // Another frame, or one input channel, is refused.
        let topo = RegridTemplate::of(&cube(0.95e9, 1.0e8, 6, FrequencyRef::TOPO, |_| 0.0));
        let error = regrid_along_frequency(&input, &topo, RegridMethod::Linear).unwrap_err();
        assert!(error.to_string().contains("spectral frames"), "{error}");
        let one = cube(1.0e9, 1.0e8, 1, FrequencyRef::LSRK, |_| 1.0);
        let error = regrid_along_frequency(&one, &template, RegridMethod::Linear).unwrap_err();
        assert!(
            error.to_string().contains("one channel") || error.to_string().contains("1 channel"),
            "{error}"
        );
        // A cube onto one channel: `_checkAxes` removes the axis of unit
        // length from those it regrids and refuses the shapes.
        let plane = RegridTemplate::of(&cube(1.0e9, 1.0e8, 1, FrequencyRef::LSRK, |_| 0.0));
        let error = regrid_along_frequency(&input, &plane, RegridMethod::Linear).unwrap_err();
        assert!(
            error.to_string().contains("same input and output shapes"),
            "{error}"
        );
    }

    /// `imregrid`, `feather` and `simutil` call [`regrid`], which keeps the
    /// spectral axis out of it: the start model's linear pass is not theirs.
    #[test]
    fn the_direction_regrid_refuses_a_spectral_axis_that_differs() {
        use casa_types::measures::frequency::FrequencyRef;
        let input = cube(1.0e9, 1.0e8, 4, FrequencyRef::LSRK, |k| k as f32);
        let template = RegridTemplate::of(&cube(0.95e9, 1.0e8, 6, FrequencyRef::LSRK, |_| 0.0));
        let error = regrid(&input, &template, RegridMethod::Linear).unwrap_err();
        assert!(error.to_string().contains("direction axes only"), "{error}");
    }

    /// Between channels the line, at a channel its value, beyond the ends
    /// the end value masked: casacore's flagged linear interpolation.
    #[test]
    fn the_line_follows_casacores_flagged_linear_interpolation() {
        let values = [1.0_f32, 3.0, 4.0, 8.0];
        let goods = [true, true, false, true];
        let at = |k: usize| (values[k], goods[k]);
        assert_eq!(linear(0.5, at, 4), (2.0, true));
        assert_eq!(linear(1.0, at, 4), (3.0, true));
        assert_eq!(linear(0.0, at, 4), (1.0, true));
        assert_eq!(linear(1.5, at, 4), (3.5, false));
        assert_eq!(linear(3.0, at, 4), (8.0, true));
        assert_eq!(linear(-0.5, at, 4), (1.0, false));
        assert_eq!(linear(4.5, at, 4), (8.0, false));
    }
}
