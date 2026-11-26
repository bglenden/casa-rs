// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType};
use casa_core::quanta::{Quantity, Unit};
use casa_core::{RecordValue, ScalarValue, Value};

use crate::convolve2d::{
    GaussianSamples, sampled_beam_convolution, sky_per_pixel, unwrap_position_angle,
};
use crate::{GaussianBeam, ImageBeamSet, ImageError, ImageInterface, MutableImageInterface};

use super::convolve::{
    ConvolutionGroups, ConvolutionOptions, ConvolutionPixel, ImageConvolution, KernelPlan,
    KernelWorkspaces,
};
use super::{SubimageOptions, SubimagePlan, SubimageRegion};

/// Requested Gaussian convolution geometry and scale.
pub struct GaussianConvolutionOptions {
    /// Pixel axes in CASA argument order. An empty list uses axes zero and one.
    pub axes: Vec<i64>,
    /// Full widths and position angle in the supplied quantity units.
    pub widths: [Quantity; 3],
    /// Positive manual multiplier, or nonpositive for unit-dependent scaling.
    pub scale: f64,
    /// Interpret the widths as the final resolution.
    pub target_resolution: bool,
}

/// A Gaussian convolution and its selected output geometry.
pub struct ImageGaussianConvolution {
    inner: ImageConvolution,
    axes: [usize; 2],
    beam_axes: [Option<usize>; 2],
    channels: usize,
    planes: Vec<Plane>,
    unit: String,
    output_info: RecordValue,
}

#[derive(Clone, Copy)]
struct Plane {
    samples: Option<GaussianSamples>,
    scale: f64,
    beam: GaussianBeam,
}

impl ImageGaussianConvolution {
    /// Resolve the selected image, physical Gaussian, and convolution scaling.
    ///
    /// # Errors
    /// The axis pair, region, kernel dimensions, or requested resolution is
    /// incompatible with the image.
    pub fn new<T: ConvolutionPixel>(
        image: &dyn ImageInterface<T>,
        native: RecordValue,
        beams: &RecordValue,
        region: Option<SubimageRegion<'_>>,
        request: GaussianConvolutionOptions,
    ) -> Result<Self, ImageError> {
        let mut axes = parse_axes(&request.axes, image.ndim())?;
        require_square(image.coordinates(), axes)?;
        let mut section = SubimagePlan::new_with_beams(
            image,
            Some(native),
            SubimageOptions {
                region,
                dropdeg: false,
                keepaxes: &[],
            },
            Some(beams),
        )?;
        let coordinates = &section.coordinates;
        let has_sky = direction_pair(coordinates).is_some_and(|sky| {
            if sky.iter().all(|axis| axes.contains(axis)) {
                axes = sky;
                true
            } else {
                false
            }
        });
        if !has_sky {
            axes.sort_unstable();
        }
        if request.target_resolution && !has_sky {
            let x = increment(coordinates, axes[0])?;
            if !x.unit().conformant(&Unit::new("rad").expect("radians")) {
                return Err(invalid(format!(
                    "Major axis must have angular units ({} is not).",
                    x.unit().name()
                )));
            }
        }
        let geometry = Sampling::new(coordinates, axes, has_sky, &request.widths)?;
        let beam_set = &section.image_info.beam_set;
        let channels = beam_set.n_channels().max(1);
        let polarizations = beam_set.n_stokes().max(1);
        let multiple = beam_set.is_multi();
        let beam_axes = [CoordinateType::Spectral, CoordinateType::Stokes].map(|kind| {
            multiple
                .then(|| image.find_axis(kind))
                .flatten()
                .filter(|axis| !axes.contains(axis))
        });
        let per_pixel = has_sky && image.units().to_ascii_uppercase().contains("/PIXEL");
        let per_beam = image.units().to_ascii_uppercase().contains("BEAM");
        let mut planes = Vec::with_capacity(channels * polarizations);
        for pol in 0..polarizations {
            for channel in 0..channels {
                planes.push(geometry.plane(
                    if beam_set.is_empty() {
                        GaussianBeam::default()
                    } else {
                        *beam_set.beam(channel, pol)
                    },
                    request.target_resolution,
                    multiple,
                    request.scale,
                    per_pixel,
                    per_beam,
                )?);
            }
        }
        let unit = if per_pixel && planes.last().is_some_and(|plane| plane.samples.is_some()) {
            "Jy/beam"
        } else {
            image.units()
        }
        .to_owned();
        let keep_requested = has_sky && (per_pixel || request.target_resolution);
        section.image_info.beam_set = if !has_sky {
            ImageBeamSet::default()
        } else if request.target_resolution {
            let beam = match planes.first() {
                Some(plane) if !plane.beam.is_null() => geometry.request,
                _ => GaussianBeam::default(),
            };
            ImageBeamSet::new(beam)
        } else if multiple {
            ImageBeamSet::from_grid(
                (0..channels)
                    .map(|channel| {
                        (0..polarizations)
                            .map(|pol| planes[pol * channels + channel].beam)
                            .collect()
                    })
                    .collect(),
            )
        } else {
            ImageBeamSet::new(planes[0].beam)
        };
        let mut output_info = section.image_info.to_record();
        if keep_requested && output_info.get("restoringbeam").is_some() {
            output_info.upsert("restoringbeam", Value::Record(geometry.beam_record()?));
        } else if per_pixel && let Some(Value::Record(grid)) = output_info.get("perplanebeams") {
            let mut grid = grid.clone();
            let record = geometry.beam_record()?;
            for index in 0..planes.len() {
                grid.upsert(format!("*{index}"), Value::Record(record.clone()));
            }
            output_info.upsert("perplanebeams", Value::Record(grid));
        }
        Ok(Self {
            inner: ImageConvolution { section },
            axes,
            beam_axes,
            channels,
            planes,
            unit,
            output_info,
        })
    }

    /// Selected pixels, shifted coordinates, and computed metadata.
    pub fn section(&self) -> &SubimagePlan {
        &self.inner.section
    }

    /// Preserve the original quantity units of a requested output beam.
    pub fn image_info_record(&self) -> &RecordValue {
        &self.output_info
    }

    /// Convolve the selected planes into a writable image of the planned shape.
    ///
    /// # Errors
    /// Input or output storage fails, or a requested mask is incompatible.
    pub fn copy_to<T: ConvolutionPixel>(
        &self,
        image: &dyn ImageInterface<T>,
        options: ConvolutionOptions<'_>,
        output: &mut dyn MutableImageInterface<T>,
    ) -> Result<(), ImageError> {
        let shape = &self.section().shape;
        let active = (0..shape.len())
            .map(|axis| shape[axis] > 1 && self.axes.contains(&axis))
            .collect::<Vec<_>>();
        let plan = |plane: Plane| {
            plane
                .samples
                .map(|samples| KernelPlan::gaussian(samples, self.axes, shape, plane.scale))
                .transpose()
        };
        if self.beam_axes.iter().all(Option::is_none)
            && let Some(workspace) = plan(self.planes[0])?
        {
            self.inner.copy_groups(
                image,
                options,
                ConvolutionGroups {
                    active: &active,
                    plane_axes: &[],
                    workspace: KernelWorkspaces::Shared(&workspace),
                },
                output,
            )?;
        } else {
            let beam_axes = self.beam_axes.into_iter().flatten().collect::<Vec<_>>();
            let plane_kernel = |position: &[usize]| {
                let [channel, pol] = self
                    .beam_axes
                    .map(|axis| axis.map_or(0, |axis| position[axis]));
                let channel = if self.channels == 1 { 0 } else { channel };
                let pol = if self.planes.len() == self.channels {
                    0
                } else {
                    pol
                };
                plan(self.planes[pol * self.channels + channel])
            };
            self.inner.copy_groups(
                image,
                options,
                ConvolutionGroups {
                    active: &active,
                    plane_axes: &beam_axes,
                    workspace: KernelWorkspaces::PerPlane(&plane_kernel),
                },
                output,
            )?;
        }
        output.set_units_string(self.unit.clone())
    }
}

fn invalid(message: impl Into<String>) -> ImageError {
    ImageError::CasaSelection(message.into())
}

fn parse_axes(axes: &[i64], rank: usize) -> Result<[usize; 2], ImageError> {
    let pair = match axes {
        [] => [0, 1],
        &[x, y] => [x, y],
        _ => return Err(invalid("Number of axes to convolve must be exactly 2")),
    };
    if pair[0] == pair[1] {
        return Err(invalid("Axes must be different"));
    }
    if pair
        .iter()
        .any(|&axis| usize::try_from(axis).map_or(true, |axis| axis >= rank))
    {
        return Err(invalid(
            "Axis value must be less than number of axes in image",
        ));
    }
    Ok(pair.map(|axis| axis as usize))
}

fn direction_pair(coordinates: &CoordinateSystem) -> Option<[usize; 2]> {
    let index = coordinates.find_coordinate(CoordinateType::Direction)?;
    if let [Some(x), Some(y)] = coordinates.coordinate_pixel_axes(index)?.as_slice() {
        Some([*x, *y])
    } else {
        None
    }
}

fn increment(coordinates: &CoordinateSystem, axis: usize) -> Result<Quantity, ImageError> {
    let (owner, local) = coordinates
        .pixel_axis_owner(axis)
        .ok_or_else(|| invalid("Selected axis has no coordinate"))?;
    let model = coordinates.coordinate(owner);
    let (value, unit) = match model {
        CoordinateModel::Direction(direction) => (
            direction.reference_and_increment_in_world_units().1[local],
            direction.world_units()[local].clone(),
        ),
        _ => (model.increment()[local], model.axis_units()[local].clone()),
    };
    Quantity::new(value.abs(), &unit).map_err(|error| invalid(error.to_string()))
}

fn require_square(coordinates: &CoordinateSystem, axes: [usize; 2]) -> Result<(), ImageError> {
    let x = increment(coordinates, axes[0])?;
    let y = increment(coordinates, axes[1])?;
    if y.get_value_in(x.unit())
        .is_ok_and(|y| (x.value() - y).abs() <= 1e-13 * x.value().max(y))
    {
        Ok(())
    } else {
        Err(invalid(
            "Pixels must be square, please regrid your image so that they are",
        ))
    }
}

struct Sampling {
    matrix: [[f64; 2]; 2],
    sky: bool,
    request: GaussianBeam,
    quantities: [Quantity; 3],
}

impl Sampling {
    fn new(
        coordinates: &CoordinateSystem,
        axes: [usize; 2],
        sky: bool,
        widths: &[Quantity; 3],
    ) -> Result<Self, ImageError> {
        let x = increment(coordinates, axes[0])?;
        let y = increment(coordinates, axes[1])?;
        let pixel_width = |q: &Quantity| matches!(q.unit().name(), "pix" | "pixel");
        let both_pixels = pixel_width(&widths[0]) && pixel_width(&widths[1]);
        if pixel_width(&widths[0]) != pixel_width(&widths[1]) {
            return Err(invalid(format!(
                "Quantum::operator< unequal units '{}', '{}'",
                widths[0].unit().name(),
                widths[1].unit().name()
            )));
        }
        let radians = Unit::new("rad").expect("radians");
        let angle = widths[2]
            .get_value_in(&radians)
            .map_err(|_| invalid("The units of the position angle must be angular"))?;
        let (matrix, values, quantities) = if sky {
            let direction = coordinates
                .find_coordinate(CoordinateType::Direction)
                .expect("sky axes");
            let CoordinateModel::Direction(direction) = coordinates.coordinate(direction) else {
                unreachable!()
            };
            let matrix = sky_per_pixel(direction)?;
            let quantities = if both_pixels {
                [
                    Quantity::with_unit(widths[0].value() * x.value(), x.unit().clone()),
                    Quantity::with_unit(widths[1].value() * y.value(), y.unit().clone()),
                    widths[2].clone(),
                ]
            } else {
                widths.clone()
            };
            let values = [
                quantities[0]
                    .get_value_in(&radians)
                    .map_err(|_| invalid("The units of the major axis must be angular"))?,
                quantities[1]
                    .get_value_in(&radians)
                    .map_err(|_| invalid("The units of the minor axis must be angular"))?,
            ];
            (matrix, values, quantities)
        } else if both_pixels {
            (
                [[0., 1.], [1., 0.]],
                [widths[0].value(), widths[1].value()],
                widths.clone(),
            )
        } else {
            let convert = |q: &Quantity| {
                q.get_value_in(x.unit()).map_err(|_| invalid(format!("The units of the world length ({}) are not consistent with those of Coordinate System ({})", q.unit().name(), x.unit().name())))
            };
            (
                [[0., convert(&y)?], [x.value(), 0.]],
                [convert(&widths[0])?, convert(&widths[1])?],
                widths.clone(),
            )
        };
        if values[0] < values[1] {
            return Err(invalid("Major axis is less than minor axis"));
        }
        if values[0] <= 0. {
            return Err(invalid(
                "Gaussian2DParam<T>::setMajorAxis(const T &width) - width must be positive",
            ));
        }
        if values[1] <= 0. {
            return Err(invalid("axialRatio must be between (0,1]"));
        }
        Ok(Self {
            matrix,
            sky,
            request: GaussianBeam::new(values[0], values[1], angle),
            quantities,
        })
    }

    fn beam_record(&self) -> Result<RecordValue, ImageError> {
        let mut beam = RecordValue::default();
        for (i, label) in ["major", "minor", "positionangle"].into_iter().enumerate() {
            let mut q = self.quantities[i].clone();
            if i == 2 {
                q = Quantity::new(
                    unwrap_position_angle(self.request.position_angle).to_degrees(),
                    "deg",
                )
                .map_err(|error| invalid(error.to_string()))?;
            }
            let mut value = RecordValue::default();
            value.upsert("value", Value::Scalar(ScalarValue::Float64(q.value())));
            value.upsert(
                "unit",
                Value::Scalar(ScalarValue::String(q.unit().name().to_owned())),
            );
            beam.upsert(label, Value::Record(value));
        }
        Ok(beam)
    }

    fn plane(
        &self,
        input: GaussianBeam,
        target: bool,
        multiple: bool,
        scale: f64,
        per_pixel: bool,
        per_beam: bool,
    ) -> Result<Plane, ImageError> {
        let input_beam = self.sky && !input.is_null();
        if target && input_beam && multiple && beams_near(self.request, input) {
            return Ok(Plane {
                samples: None,
                scale: 1.,
                beam: input,
            });
        }
        let beam = if target && input_beam {
            self.request.deconvolving_beam(input)
                .ok().flatten().ok_or_else(|| invalid("Unable to reach target resolution. Input image beam is identical to or larger than the output beam size"))?
        } else {
            self.request
        };
        let samples = GaussianSamples::new(beam, self.matrix)?;
        let automatic = scale <= 0.;
        let (scale, beam) = if per_pixel {
            (if automatic { 1. } else { scale }, self.request)
        } else if input_beam {
            let input_samples = GaussianSamples::new(input, self.matrix)?;
            let (peak, fitted) = sampled_beam_convolution(input_samples, samples, self.matrix)?;
            let scale = if automatic { peak.recip() } else { scale };
            let scale = if per_beam {
                scale
            } else {
                scale * input.area() / fitted.area()
            };
            let beam = if target { self.request } else { fitted };
            (scale, beam)
        } else {
            let scale = if automatic {
                samples.volume().recip()
            } else {
                scale
            };
            (scale, GaussianBeam::default())
        };
        Ok(Plane {
            samples: Some(samples),
            scale,
            beam,
        })
    }
}

fn beams_near(a: GaussianBeam, b: GaussianBeam) -> bool {
    let close_width = |a: f64, b: f64| (a - b).abs() <= 1e-5 * a.abs().max(b.abs());
    let angular_tolerance = (0.01_f64 / 3600.).to_radians();
    close_width(a.major, b.major)
        && close_width(a.minor, b.minor)
        && (unwrap_position_angle(a.position_angle) - unwrap_position_angle(b.position_angle)).abs()
            <= angular_tolerance
}
