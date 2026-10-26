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
//! reference frequency. An image in Jy/beam gains the components times the
//! beam's area in pixels; any other unit without "pixel" in it is taken as
//! Jy/pixel, with CASA's warning. As in CASA, a spectral or Stokes axis that
//! is the image's first is not treated as one, and the image's mask is not
//! consulted.
//!
//! The arithmetic is CASA's single precision: the components' sum in double
//! precision, rounded, plus the points' (summed in single precision), added
//! to the image.

use std::collections::BTreeMap;
use std::path::Path;

use casa_components::{ComponentList, PixelGrid, ShapeKind, SkyComponent};
use casa_coordinates::{Coordinate, CoordinateModel, CoordinateType, DirectionCoordinate};
use casa_images::PagedImage;
use casa_types::measures::direction::MDirection;
use casa_types::measures::frame::MeasFrame;
use ndarray::{Array3, IxDyn};

use super::{SkyModelError, pixel_axes};
use crate::simutil::{SimutilError, qa};

/// The image's grid, as the components are placed on it.
struct Placement<'a> {
    direction: &'a DirectionCoordinate,
    grid: PixelGrid,
    shape: [usize; 2],
    frequencies_hz: Vec<f64>,
}

impl Placement<'_> {
    /// The direction cosines of pixel `(x, y)`.
    fn pixel_direction(&self, x: f64, y: f64) -> Result<[f64; 3], SkyModelError> {
        let world = self.direction.to_world(&[x, y]).map_err(|_| {
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
        let pixel = self.direction.to_pixel(&[lon, lat])?;
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
        for [x, y] in candidates {
            let mut values = Array3::zeros((4, 1, self.frequencies_hz.len()));
            let at = self.pixel_direction(x, y)?;
            point.sample_pixels(&mut values, &[at], &self.grid, &self.frequencies_hz)?;
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
    if list.is_empty() {
        return Err(SkyModelError::Model(
            "There are no components in the model componentlist".to_string(),
        ));
    }
    let mut image = PagedImage::<f32>::open(image_path)?;
    let coordinates = image.coordinates().clone();
    let shape = image.shape().to_vec();
    let Some((direction_index, direction_axes)) =
        pixel_axes(&coordinates, CoordinateType::Direction)
    else {
        return Err(SkyModelError::Model(
            "Image does not have a direction coordinate".to_string(),
        ));
    };
    let CoordinateModel::Direction(direction) = coordinates.coordinate(direction_index) else {
        unreachable!("the coordinate found is a direction coordinate");
    };
    let increment = direction.increment();
    let mut placement = Placement {
        direction,
        grid: PixelGrid {
            frame: direction.direction_ref(),
            longitude_size_rad: increment[0].abs(),
            latitude_size_rad: increment[1].abs(),
        },
        shape: [shape[direction_axes[0]], shape[direction_axes[1]]],
        frequencies_hz: Vec::new(),
    };
    let spectral =
        pixel_axes(&coordinates, CoordinateType::Spectral).filter(|(_, axes)| axes[0] > 0);
    placement.frequencies_hz = match &spectral {
        Some((index, axes)) => {
            let CoordinateModel::Spectral(coordinate) = coordinates.coordinate(*index) else {
                unreachable!("the coordinate found is a spectral coordinate");
            };
            let to_hz = qa_value_in_hz(&coordinate.axis_units()[0])?;
            (0..shape[axes[0]])
                .map(|channel| Ok(coordinate.to_world(&[channel as f64])?[0] * to_hz))
                .collect::<Result<_, SkyModelError>>()?
        }
        None => vec![list.components()[0].spectrum().reference_frequency().hz()],
    };
    let stokes = stokes_planes(&coordinates, &shape)?;

    let mut extended = Vec::new();
    let mut points: BTreeMap<[usize; 2], Vec<f32>> = BTreeMap::new();
    let nfreq = placement.frequencies_hz.len();
    let nstokes = stokes.planes.len();
    for component in list.iter() {
        if !matches!(component.shape().kind(), ShapeKind::Point) {
            extended.push(component);
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
            PointPlacement::Nowhere => extended.push(component),
        }
    }

    let [nx, ny] = placement.shape;
    let mut directions = Vec::with_capacity(nx * ny);
    for y in 0..ny {
        for x in 0..nx {
            directions.push(placement.pixel_direction(x as f64, y as f64)?);
        }
    }
    let mut samples = Array3::<f64>::zeros((4, nx * ny, nfreq));
    for component in extended {
        component.sample_pixels(
            &mut samples,
            &directions,
            &placement.grid,
            &placement.frequencies_hz,
        )?;
    }

    let scale = unit_scale(&image, direction)?;
    let mut pixels = image.get()?;
    let frequency_axis = spectral.as_ref().map(|(_, axes)| axes[0]);
    let mut position = vec![0; shape.len()];
    for y in 0..ny {
        for x in 0..nx {
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
                    let mut value = samples[[parameter, y * nx + x, frequency]] as f32;
                    if let Some(point) = point {
                        value += point[frequency * nstokes + plane];
                    }
                    if let Some(scale) = scale {
                        value = (f64::from(value) * scale) as f32;
                    }
                    pixels[IxDyn(&position)] += value;
                }
            }
        }
    }
    image.put_slice(&pixels, &vec![0; shape.len()])?;
    image.save()?;
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
    image: &PagedImage<f32>,
    direction: &DirectionCoordinate,
) -> Result<Option<f64>, SkyModelError> {
    let units = image.units().to_string();
    if units.contains("pixel") {
        return Ok(None);
    }
    if !units.contains("beam") {
        tracing::warn!(
            "Image units [{units}] are not dimensionally equivalent to Jy/pixel or Jy/beam. Assuming they are equivalent Jy/pixel"
        );
        return Ok(None);
    }
    let info = image.image_info()?;
    if info.beam_set.has_multi_beam() {
        return Err(SkyModelError::Model(
            "casa-rs does not yet add components to an image with a beam per plane".to_string(),
        ));
    }
    let Some(beam) = info.beam_set.single_beam().filter(|beam| !beam.is_null()) else {
        tracing::warn!(
            "No beam defined even though the image units contain a beam. Will assume the beam area is one pixel"
        );
        return Ok(None);
    };
    let increment = direction.increment();
    Ok(Some(beam.area() / (increment[0] * increment[1]).abs()))
}
