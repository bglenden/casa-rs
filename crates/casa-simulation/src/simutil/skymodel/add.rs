// SPDX-License-Identifier: LGPL-3.0-or-later
//! `ia.modify(cl.torecord(), subtract=False)`: a component list added to an
//! image, as CASA's `ComponentImager::modify` adds it through a
//! `ComponentListImage` (`imageanalysis/Images`).
//!
//! The components are sampled at each pixel's direction
//! ([`SkyComponent::sample_pixels`]) and frequency, a Stokes plane taking the
//! Stokes parameter it is labelled with. A point goes whole into its nearest
//! pixel: the pixel whose centre it lies within half a pixel of, looked for
//! at the rounded pixel and then the eight around it. A point whose pixel
//! is off the image is left out, as is one more than a pixel beyond its
//! edge. A point no pixel holds, and every other shape, is sampled at every
//! pixel.
//!
//! Without a spectral axis every component is sampled at the first one's
//! reference frequency. With one, the frequencies are the image's world
//! frequencies, in its conversion layer's frame when it has one, and a
//! component whose spectrum is in another frame is sampled at them
//! expressed in its own: CASA converts the reference frequency into the
//! image's frame instead (`refFreqInFrame`), which gives the same ratio.
//! The conversion uses the conversion layer's epoch, position and
//! direction. An image without a usable frame, one with no conversion layer
//! or with the placeholder `ia.fromshape` leaves (an epoch of 0 at the
//! geocentre), is sampled with the frequencies unconverted, where CASA
//! converts through the placeholder (`docs/CASA (C++) bugs.md`).
//!
//! An image in Jy/beam gains the components times the
//! beam's area in pixels; any other unit without "pixel" in it is taken as
//! Jy/pixel, with CASA's warning. As in CASA, a spectral or Stokes axis that
//! is the image's first is not treated as one, and the image's mask is not
//! consulted.
//!
//! The arithmetic is CASA's single precision: the components' sum in double
//! precision, rounded, plus the points' (summed in single precision), added
//! to the image.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::path::Path;

use casa_components::{ComponentList, PixelGrid, ShapeKind, SkyComponent};
use casa_coordinates::{Coordinate, CoordinateModel, CoordinateType, DirectionCoordinate};
use casa_images::analysis::{SubimageOptions, SubimagePlan, SubimageRegion};
use casa_images::{ImageError, ImageInfo, MutableImageInterface, PagedImage};
use casa_types::measures::direction::MDirection;
use casa_types::measures::frame::MeasFrame;
use casa_types::measures::frequency::FrequencyRef;
use ndarray::{Array3, ArrayD, IxDyn};

use super::{SkyModelError, pixel_axes, require_native_direction_frame};
use crate::simutil::{SimutilError, qa};

/// The image's grid, as the components are placed on it.
struct Placement<'a> {
    direction: &'a DirectionCoordinate,
    grid: PixelGrid,
    shape: [usize; 2],
    frequencies_hz: Vec<f64>,
    /// The frame of `frequencies_hz` and, when it is usable, the measures
    /// frame to convert a component's reference frequency into it; `None`
    /// without a spectral axis.
    spectral_frame: Option<(FrequencyRef, Option<MeasFrame>)>,
}

impl Placement<'_> {
    /// The frequencies at which `component` is sampled: the image's,
    /// expressed in the frame of the component's spectrum (see the module
    /// documentation).
    fn frequencies_for(&self, component: &SkyComponent) -> Result<Cow<'_, [f64]>, SkyModelError> {
        let reference = component.spectrum().reference_frequency();
        let unconverted = Ok(Cow::Borrowed(self.frequencies_hz.as_slice()));
        let Some((image_frame, measures)) = &self.spectral_frame else {
            return unconverted;
        };
        let rest = |frame: FrequencyRef| frame == FrequencyRef::REST;
        if reference.refer() == *image_frame || rest(reference.refer()) || rest(*image_frame) {
            return unconverted;
        }
        let Some(measures) = measures else {
            return unconverted;
        };
        let converted = reference.convert_to(*image_frame, measures).map_err(|_| {
            SkyModelError::Model(format!(
                "Cannot transform frequency from {} to {}",
                image_frame.as_str(),
                reference.refer().as_str()
            ))
        })?;
        let ratio = reference.hz() / converted.hz();
        Ok(Cow::Owned(
            self.frequencies_hz
                .iter()
                .map(|frequency| frequency * ratio)
                .collect(),
        ))
    }

    /// The direction cosines of pixel `(x, y)`.
    fn pixel_direction(&self, x: f64, y: f64) -> Result<[f64; 3], SkyModelError> {
        let world = self.direction.pixel_to_world([x, y]).map_err(|_| {
            SkyModelError::Model(format!(
                "Unable to convert to world direction at pixel [{x}, {y}]"
            ))
        })?;
        Ok(MDirection::from_angles(world[0], world[1], self.grid.frame).cosines())
    }

    /// Where a point goes (`_findPointSoures`): its pixel and its sampled
    /// values `[Stokes, frequency]`, `None` when its pixel is off the
    /// image, or an error that it lies in no pixel (to be sampled at all of
    /// them).
    fn point_pixel(&self, point: &SkyComponent) -> Result<PointPlacement, SkyModelError> {
        let direction = point
            .shape()
            .reference_direction()
            .convert_to(self.grid.frame, &MeasFrame::default())
            .map_err(|error| SkyModelError::Model(error.to_string()))?;
        let (lon, lat) = direction.as_angles();
        let pixel = self.direction.world_to_pixel([lon, lat])?;
        let target = [(pixel[0] + 0.5).floor(), (pixel[1] + 0.5).floor()];
        let [nx, ny] = self.shape.map(|n| n as f64);
        if target[0] < -1.0 || target[1] < -1.0 || target[0] > nx || target[1] > ny {
            return Ok(PointPlacement::Outside);
        }
        let mut candidates = vec![target];
        for dx in -1..=1 {
            for dy in -1..=1 {
                if dx != 0 || dy != 0 {
                    candidates.push([target[0] + f64::from(dx), target[1] + f64::from(dy)]);
                }
            }
        }
        let frequencies = self.frequencies_for(point)?;
        for [x, y] in candidates {
            let mut values = Array3::zeros((4, 1, frequencies.len()));
            let at = self.pixel_direction(x, y)?;
            point.sample_pixels(&mut values, &[at], &self.grid, &frequencies)?;
            if values.iter().any(|value| *value != 0.0) {
                let inside = x >= 0.0 && y >= 0.0 && x < nx && y < ny;
                return Ok(if inside {
                    PointPlacement::At([x as usize, y as usize], values)
                } else {
                    PointPlacement::Outside
                });
            }
        }
        Ok(PointPlacement::Nowhere)
    }
}

enum PointPlacement {
    At([usize; 2], Array3<f64>),
    Outside,
    Nowhere,
}

/// `ia.modify(model, subtract=False)` with the components of `list`: see
/// the module documentation.
///
/// # Errors
///
/// An empty list, an image with no direction coordinate, a Stokes plane
/// other than I, Q, U or V, a component casa-rs does not sample, or an image
/// that cannot be read or written.
pub fn add_components(image_path: &Path, list: &ComponentList) -> Result<(), SkyModelError> {
    require_components(list)?;
    let mut image = PagedImage::<f32>::open(image_path)?;
    let (direction, axes) = model_direction(image.coordinates())?;
    require_native_direction_frame(direction)?;
    let info = image.image_info()?;
    let (scale, warning) = unit_scale(
        image.units(),
        &info,
        direction,
        "casa-rs does not yet add components to an image with a beam per plane",
    )?;
    let mut pixels = image.get()?;
    apply_model(
        &mut pixels,
        image.coordinates(),
        direction,
        axes,
        list,
        scale,
        false,
    )?;
    if let Some(warning) = warning {
        tracing::warn!("{warning}");
    }
    image.put_slice(&pixels, &vec![0; image.shape().len()])?;
    image.save()?;
    Ok(())
}

/// Selection and arithmetic for a public image component edit.
pub struct ComponentEdit<'a> {
    /// Restrict edits to the selected region's bounding section.
    pub region: Option<SubimageRegion<'a>>,
    /// Optional lattice mask to check for conformability.
    pub mask: Option<&'a str>,
    /// Permit degenerate input mask axes to expand.
    pub stretch: bool,
    /// Remove the model from the image.
    pub subtract: bool,
}

/// The selected pixel bounds and any brightness-unit warning.
pub struct ComponentEditReport {
    /// Inclusive bounds in the parent image.
    pub bounds: (Vec<usize>, Vec<usize>),
    /// CASA's warning when the brightness unit implies an assumed beam area.
    pub warning: Option<String>,
}

/// Apply a component model in the selected section.
///
/// # Errors
///
/// Invalid selections or model geometry, unsupported image metadata, and
/// failures to read or write the image section.
pub fn modify_components(
    image: &mut dyn MutableImageInterface<f32>,
    list: &ComponentList,
    options: ComponentEdit<'_>,
) -> Result<ComponentEditReport, SkyModelError> {
    if image
        .coordinates()
        .find_coordinate(CoordinateType::Direction)
        .is_none()
    {
        let name = image
            .name()
            .map_or_else(|| "Temporary_Image".into(), Path::to_string_lossy);
        return Err(SkyModelError::Model(format!(
            "Exception: Image {name} does not have required coordinate Direction."
        )));
    }
    require_components(list)?;
    let plan = SubimagePlan::new(
        image,
        None,
        SubimageOptions {
            region: options.region,
            dropdeg: false,
            keepaxes: &[],
        },
    )?;
    plan.validate_mask(image, options.mask, options.stretch)?;
    let (direction, axes) = model_direction(&plan.coordinates)?;
    let (scale, warning) = unit_scale(
        image.units(),
        plan.image_info(),
        direction,
        "LELCoordinates::getSpectralInfo - cannot extend lattice without coordinates",
    )?;
    let (start, end) = plan.bounds();
    let mut pixels = image
        .get_slice(start, &plan.shape, &vec![1; plan.shape.len()])
        .map_err(ImageError::from)?;
    apply_model(
        &mut pixels,
        &plan.coordinates,
        direction,
        axes,
        list,
        scale,
        options.subtract,
    )?;
    image.put_slice(&pixels, start).map_err(ImageError::from)?;
    Ok(ComponentEditReport {
        bounds: (start.to_vec(), end),
        warning,
    })
}

fn require_components(list: &ComponentList) -> Result<(), SkyModelError> {
    if list.is_empty() {
        return Err(SkyModelError::Model(
            "There are no components in the model componentlist".into(),
        ));
    }
    Ok(())
}

fn model_direction(
    coordinates: &casa_coordinates::CoordinateSystem,
) -> Result<(&DirectionCoordinate, [usize; 2]), SkyModelError> {
    let Some((direction_index, direction_axes)) =
        pixel_axes(coordinates, CoordinateType::Direction)
    else {
        return Err(SkyModelError::Model(
            "Image does not have a direction coordinate".to_string(),
        ));
    };
    let CoordinateModel::Direction(direction) = coordinates.coordinate(direction_index) else {
        unreachable!("the coordinate found is a direction coordinate");
    };
    let axes = direction_axes
        .try_into()
        .map_err(|_| SkyModelError::Model("Image does not have two direction axes".into()))?;
    Ok((direction, axes))
}

fn apply_model(
    pixels: &mut ArrayD<f32>,
    coordinates: &casa_coordinates::CoordinateSystem,
    direction: &DirectionCoordinate,
    direction_axes: [usize; 2],
    list: &ComponentList,
    scale: Option<f64>,
    subtract: bool,
) -> Result<(), SkyModelError> {
    let shape = pixels.shape();
    let increment = direction.increment();
    let mut placement = Placement {
        direction,
        grid: PixelGrid {
            frame: direction.conversion_direction_ref(),
            longitude_size_rad: increment[0].abs(),
            latitude_size_rad: increment[1].abs(),
        },
        shape: [shape[direction_axes[0]], shape[direction_axes[1]]],
        frequencies_hz: Vec::new(),
        spectral_frame: None,
    };
    let spectral =
        pixel_axes(coordinates, CoordinateType::Spectral).filter(|(_, axes)| axes[0] > 0);
    placement.frequencies_hz = match &spectral {
        Some((index, axes)) => {
            let CoordinateModel::Spectral(coordinate) = coordinates.coordinate(*index) else {
                unreachable!("the coordinate found is a spectral coordinate");
            };
            let usable = coordinate
                .conversion_frame()
                .filter(|frame| usable_frame(frame))
                .cloned();
            placement.spectral_frame = Some((coordinate.world_frequency_ref(), usable));
            let to_hz = qa_value_in_hz(&coordinate.axis_units()[0])?;
            (0..shape[axes[0]])
                .map(|channel| Ok(coordinate.to_world(&[channel as f64])?[0] * to_hz))
                .collect::<Result<_, SkyModelError>>()?
        }
        None => vec![list.components()[0].spectrum().reference_frequency().hz()],
    };
    let stokes = stokes_planes(coordinates, shape)?;

    let mut extended = Vec::new();
    let mut points: BTreeMap<[usize; 2], Vec<f32>> = BTreeMap::new();
    let nfreq = placement.frequencies_hz.len();
    let nstokes = stokes.planes.len();
    for component in list.iter() {
        if !matches!(component.shape().kind(), ShapeKind::Point) {
            extended.push((component, placement.frequencies_for(component)?));
            continue;
        }
        match placement.point_pixel(component)? {
            PointPlacement::At(pixel, values) => {
                let sums = points
                    .entry(pixel)
                    .or_insert_with(|| vec![0.0; nfreq * nstokes]);
                for frequency in 0..nfreq {
                    for (plane, &parameter) in stokes.planes.iter().enumerate() {
                        let sum = &mut sums[frequency * nstokes + plane];
                        *sum = (f64::from(*sum) + values[[parameter, 0, frequency]]) as f32;
                    }
                }
            }
            PointPlacement::Outside => {}
            PointPlacement::Nowhere => {
                extended.push((component, placement.frequencies_for(component)?))
            }
        }
    }

    let [nx, ny] = placement.shape;
    let frequency_axis = spectral.as_ref().map(|(_, axes)| axes[0]);
    let mut position = vec![0; shape.len()];
    // Bound the temporary double-precision model to about one MiB per batch.
    let spatial_batch = (131_072 / (4 * nfreq)).max(1);
    for first in (0..nx * ny).step_by(spatial_batch) {
        let length = spatial_batch.min(nx * ny - first);
        let samples = if extended.is_empty() {
            None
        } else {
            let directions = (first..first + length)
                .map(|pixel| placement.pixel_direction((pixel % nx) as f64, (pixel / nx) as f64))
                .collect::<Result<Vec<_>, _>>()?;
            let mut samples = Array3::<f64>::zeros((4, length, nfreq));
            for (component, frequencies) in &extended {
                component.sample_pixels(&mut samples, &directions, &placement.grid, frequencies)?;
            }
            Some(samples)
        };
        for local in 0..length {
            let x = (first + local) % nx;
            let y = (first + local) / nx;
            let point = points.get(&[x, y]);
            for frequency in 0..nfreq {
                for (plane, &parameter) in stokes.planes.iter().enumerate() {
                    position[direction_axes[0]] = x;
                    position[direction_axes[1]] = y;
                    if let Some(axis) = frequency_axis {
                        position[axis] = frequency;
                    }
                    if let Some(axis) = stokes.axis {
                        position[axis] = plane;
                    }
                    let mut value = samples
                        .as_ref()
                        .map_or(0.0, |samples| samples[[parameter, local, frequency]] as f32);
                    if let Some(point) = point {
                        value += point[frequency * nstokes + plane];
                    }
                    if let Some(scale) = scale {
                        value = (f64::from(value) * scale) as f32;
                    }
                    pixels[IxDyn(&position)] += if subtract { -value } else { value };
                }
            }
        }
    }
    Ok(())
}

/// The image's Stokes axis and, for each of its planes, the Stokes
/// parameter (0 to 3 for I, Q, U, V) it takes; one plane of I without one.
struct StokesPlanes {
    axis: Option<usize>,
    planes: Vec<usize>,
}

fn stokes_planes(
    coordinates: &casa_coordinates::CoordinateSystem,
    shape: &[usize],
) -> Result<StokesPlanes, SkyModelError> {
    let Some((index, axes)) =
        pixel_axes(coordinates, CoordinateType::Stokes).filter(|(_, axes)| axes[0] > 0)
    else {
        return Ok(StokesPlanes {
            axis: None,
            planes: vec![0],
        });
    };
    let CoordinateModel::Stokes(coordinate) = coordinates.coordinate(index) else {
        unreachable!("the coordinate found is a Stokes coordinate");
    };
    let planes = (0..shape[axes[0]])
        .map(|plane| {
            let name = coordinate
                .stokes()
                .get(plane)
                .map_or("", |stokes| stokes.name());
            ["I", "Q", "U", "V"]
                .iter()
                .position(|parameter| *parameter == name)
                .ok_or_else(|| {
                    SkyModelError::Model(format!(
                        "Unsupported stokes value {name} at pixel {plane}"
                    ))
                })
        })
        .collect::<Result<_, _>>()?;
    Ok(StokesPlanes {
        axis: Some(axes[0]),
        planes,
    })
}

/// Whether a conversion layer's measures frame is one to convert with: an
/// epoch and a position, neither the placeholder a spectral coordinate made
/// from a shape holds (MJD 0, the geocentre), and a direction.
fn usable_frame(frame: &MeasFrame) -> bool {
    let epoch = frame
        .epoch()
        .is_some_and(|epoch| epoch.value().as_mjd() != 0.0);
    let position = frame
        .position()
        .is_some_and(|position| position.as_itrf().iter().any(|value| *value != 0.0));
    epoch && position && frame.direction().is_some()
}

/// Hz per unit of a spectral axis in `unit`.
fn qa_value_in_hz(unit: &str) -> Result<f64, SkyModelError> {
    let one = casa_types::quanta::Quantity::with_unit(
        1.0,
        casa_types::quanta::casacore::casacore_unit(unit).map_err(SimutilError::from)?,
    );
    Ok(qa::value_in(&one, "Hz")?)
}

/// The factor the components are scaled by for the image's brightness
/// unit: the beam's area in pixels for Jy/beam with a beam, none otherwise.
fn unit_scale(
    units: &str,
    info: &ImageInfo,
    direction: &DirectionCoordinate,
    multi_beam_error: &str,
) -> Result<(Option<f64>, Option<String>), SkyModelError> {
    if units.contains("pixel") {
        return Ok((None, None));
    }
    if !units.contains("beam") {
        return Ok((
            None,
            Some(format!(
                "Image units [{units}] are not dimensionally equivalent to Jy/pixel or Jy/beam. Assuming they are equivalent Jy/pixel"
            )),
        ));
    }
    if info.beam_set.has_multi_beam() {
        return Err(SkyModelError::Model(multi_beam_error.into()));
    }
    let Some(beam) = info.beam_set.single_beam().filter(|beam| !beam.is_null()) else {
        return Ok((None, Some(
            "No beam defined even though the image units contain a beam. Will assume the beam area is one pixel".into()
        )));
    };
    let increment = direction.increment();
    Ok((
        Some(beam.area() / (increment[0] * increment[1]).abs()),
        None,
    ))
}
