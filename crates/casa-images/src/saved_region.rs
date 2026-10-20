// SPDX-License-Identifier: LGPL-3.0-or-later
//! World-coordinate polygon regions saved with an image.
//!
//! casacore keeps named regions in an image's `regions` table keyword
//! (C++ `ImageInterface::defineRegion` and `getRegion`, through
//! `RegionHandler`). This module reads and writes the world-coordinate
//! polygon classes stored there, a single `WCPolygon` or a `WCUnion` whose
//! members are all `WCPolygon`s, and turns a region into a pixel mask or into
//! statistics over the pixels it selects.
//!
//! | Rust                                   | C++ casacore                                   |
//! |----------------------------------------|------------------------------------------------|
//! | [`PolygonRegion`]                      | `WCPolygon`, `WCUnion` of `WCPolygon`          |
//! | [`PagedImage::get_polygon_region`]     | `ImageInterface::getRegion`                    |
//! | [`PagedImage::put_polygon_region`]     | `ImageInterface::defineRegion` (`Regions`)     |
//! | [`PolygonRegion::mask`]                | `WCRegion::toLCRegion` then `LCRegion::getMask` |
//! | [`PagedImage::region_statistics`]      | statistics of `SubImage(image, region)`        |
//!
//! As in casacore's `WCRegion::toLCRegion`, a polygon drawn on two pixel axes
//! covers the full extent of every other image axis. Its vertices are
//! converted to pixel coordinates plane by plane, at the centre of the
//! polygon axes and the plane's position on the others. A pixel belongs to a
//! polygon when its centre lies inside it or on its boundary.
//!
//! Other casacore region classes (`WCBox`, `WCEllipsoid`, the `LC*` pixel
//! regions and compounds other than a union of polygons) are reported by
//! class name and not converted. Removing a saved region is
//! [`PagedImage::remove_region`].
//!
//! # Example
//!
//! ```rust
//! use casa_coordinates::{CoordinateSystem, DirectionCoordinate, Projection, ProjectionType};
//! use casa_images::{PagedImage, PolygonRegion};
//! use casa_types::measures::direction::DirectionRef;
//!
//! let mut coords = CoordinateSystem::new();
//! coords.add_coordinate(DirectionCoordinate::new(
//!     DirectionRef::J2000,
//!     Projection::new(ProjectionType::SIN),
//!     [0.0, 0.5],
//!     [-1e-4, 1e-4],
//!     [2.0, 2.0],
//! ));
//! let dir = tempfile::tempdir().unwrap();
//! let mut image = PagedImage::<f32>::create(vec![5, 5], coords.clone(), dir.path().join("a.image")).unwrap();
//!
//! let mut region = PolygonRegion::new(&coords, [0, 1]).unwrap();
//! let corners = [[1.0, 1.0], [3.0, 1.0], [3.0, 3.0], [1.0, 3.0]];
//! region
//!     .add_polygon(corners.iter().map(|&[x, y]| {
//!         let world = coords.to_world(&[x, y]).unwrap();
//!         [world[0], world[1]]
//!     }).collect())
//!     .unwrap();
//! image.put_polygon_region("box", &region).unwrap();
//! image.save().unwrap();
//!
//! let mask = region.mask(&coords, image.shape()).unwrap();
//! assert_eq!(mask.iter().filter(|&&inside| inside).count(), 9);
//! assert_eq!(image.get_polygon_region("box").unwrap().polygons.len(), 1);
//! ```

mod geometry;
mod record;

use casa_coordinates::CoordinateSystem;
use casa_lattices::StatsElement;
use casa_types::RecordValue;
use ndarray::{Array2, ArrayD, Dimension, IxDyn, Slice};

use self::geometry::{enumerate_axis_indices, polygon_contains_pixel, validate_polygon};
use crate::error::ImageError;
use crate::image::{ImagePixel, PagedImage};

/// A region made of world-coordinate polygons on two pixel axes.
///
/// One polygon is saved as a casacore `WCPolygon`; several are saved as a
/// `WCUnion` of `WCPolygon`s, and the region is their union. Each vertex is
/// `[x, y]` in world coordinates on [`pixel_axes`](Self::pixel_axes),
/// expressed in [`units`](Self::units). Polygons are closed implicitly: the
/// last vertex joins the first.
#[derive(Debug, Clone, PartialEq)]
pub struct PolygonRegion {
    /// The two zero-based image pixel axes the polygons are drawn on.
    pub pixel_axes: [usize; 2],
    /// World units of the vertex coordinates on each axis, such as `"rad"`.
    pub units: [String; 2],
    /// The polygons, each a list of `[x, y]` world-coordinate vertices.
    pub polygons: Vec<Vec<[f64; 2]>>,
}

/// Statistics of the pixels a region selects.
///
/// A pixel counts when it lies inside the region, the image's default mask
/// (if any) selects it, and its value is finite. This is the data C++
/// casacore returns from a `SubImage` of the image over the region, with
/// non-finite values dropped.
#[derive(Debug, Clone, PartialEq)]
pub struct RegionStatistics {
    /// Number of pixels used.
    pub pixel_count: usize,
    /// Sum of the values.
    pub sum: f64,
    /// Mean value.
    pub mean: f64,
    /// Median value; the mean of the two middle values for an even count.
    pub median: f64,
    /// Root mean square of the values.
    pub rms: f64,
    /// Population standard deviation about the mean (divides by the count).
    pub sigma: f64,
    /// Smallest value.
    pub min: f64,
    /// Largest value.
    pub max: f64,
    /// The image brightness unit, such as `"Jy/beam"`.
    pub unit: String,
}

/// Polygons as lists of `[x, y]` pixel coordinates on the region axes.
type PixelPolygons = Vec<Vec<[f64; 2]>>;

/// The pixels a region selects in one plane: the bounding box of its
/// polygons in that plane, and which pixels of the box are inside.
struct PlaneSelection {
    start: Vec<usize>,
    extent: Vec<usize>,
    inside: Array2<bool>,
}

impl PolygonRegion {
    /// Creates an empty region on `pixel_axes`, taking the vertex units from
    /// the world axes of `coords`.
    pub fn new(coords: &CoordinateSystem, pixel_axes: [usize; 2]) -> Result<Self, ImageError> {
        let units = world_axis_units(coords);
        check_pixel_axes(pixel_axes, units.len())?;
        Ok(Self {
            pixel_axes,
            units: [units[pixel_axes[0]].clone(), units[pixel_axes[1]].clone()],
            polygons: Vec::new(),
        })
    }

    /// Adds a polygon given by its `[x, y]` world-coordinate vertices.
    ///
    /// The polygon needs at least three vertices and must not cross itself.
    pub fn add_polygon(&mut self, vertices: Vec<[f64; 2]>) -> Result<(), ImageError> {
        validate_polygon(&vertices)?;
        self.polygons.push(vertices);
        Ok(())
    }

    /// Encodes the region as a casacore region record.
    ///
    /// One polygon becomes a `WCPolygon` record and several a `WCUnion`. The
    /// record embeds `coords`, the coordinate system of the image the region
    /// is saved with, as casacore does.
    pub fn to_record(&self, coords: &CoordinateSystem) -> Result<RecordValue, ImageError> {
        record::encode(self, coords)
    }

    /// Decodes the casacore region record saved as `name` with an image whose
    /// coordinate system is `coords`.
    ///
    /// Accepts a `WCPolygon` with absolute coordinates, or a `WCUnion` whose
    /// members are such polygons on the same pixel axes. Vertex values are
    /// converted to the units of the image's world axes. Any other class is
    /// an error that names the region and the class.
    pub fn from_record(
        name: &str,
        record: &RecordValue,
        coords: &CoordinateSystem,
    ) -> Result<Self, ImageError> {
        record::decode(name, record, coords)
    }

    /// Returns a mask of an image of `shape` with coordinate system `coords`
    /// that is `true` for every pixel inside the region.
    ///
    /// The polygons cover the full extent of every axis other than the
    /// region's pixel axes.
    pub fn mask(
        &self,
        coords: &CoordinateSystem,
        shape: &[usize],
    ) -> Result<ArrayD<bool>, ImageError> {
        let mut mask = ArrayD::from_elem(IxDyn(shape), false);
        self.visit_planes(coords, shape, |plane| {
            let [x_axis, y_axis] = self.pixel_axes;
            let mut view = mask.slice_each_axis_mut(|axis| {
                let start = plane.start[axis.axis.index()];
                Slice::from(start..start + plane.extent[axis.axis.index()])
            });
            for (index, value) in view.indexed_iter_mut() {
                *value = plane.inside[[index[x_axis], index[y_axis]]];
            }
            Ok(())
        })?;
        Ok(mask)
    }

    fn check_axes(&self, coords: &CoordinateSystem) -> Result<(), ImageError> {
        check_pixel_axes(self.pixel_axes, coords.n_pixel_axes())
    }

    /// Calls `visit` for every plane in which the region selects at least
    /// one pixel.
    fn visit_planes(
        &self,
        coords: &CoordinateSystem,
        shape: &[usize],
        mut visit: impl FnMut(&PlaneSelection) -> Result<(), ImageError>,
    ) -> Result<(), ImageError> {
        if coords.n_pixel_axes() != shape.len() {
            return Err(ImageError::InvalidMetadata(format!(
                "polygon regions need a coordinate system for every image axis; the image has {} axes and its coordinate system {}",
                shape.len(),
                coords.n_pixel_axes()
            )));
        }
        self.check_axes(coords)?;
        for polygon in &self.polygons {
            validate_polygon(polygon)?;
        }
        let [x_axis, y_axis] = self.pixel_axes;
        let other_axes = (0..shape.len())
            .filter(|&axis| axis != x_axis && axis != y_axis)
            .collect::<Vec<_>>();
        let lengths = other_axes
            .iter()
            .map(|&axis| shape[axis])
            .collect::<Vec<_>>();
        let mut base_pixel = shape
            .iter()
            .map(|&len| (len.saturating_sub(1) / 2) as f64)
            .collect::<Vec<_>>();
        let mut previous: Option<(PixelPolygons, Option<PlaneSelection>)> = None;
        for context in enumerate_axis_indices(&lengths) {
            for (&axis, &index) in other_axes.iter().zip(&context) {
                base_pixel[axis] = index as f64;
            }
            let polygons = self.plane_pixel_polygons(coords, &base_pixel)?;
            let reuse = previous
                .as_ref()
                .is_some_and(|(previous_polygons, _)| *previous_polygons == polygons);
            if !reuse {
                let selection = select_plane(&polygons, shape, self.pixel_axes);
                previous = Some((polygons, selection));
            }
            let Some((_, Some(selection))) = previous.as_mut() else {
                continue;
            };
            for (&axis, &index) in other_axes.iter().zip(&context) {
                selection.start[axis] = index;
            }
            visit(selection)?;
        }
        Ok(())
    }

    /// Converts the polygons to pixel coordinates on the region axes in the
    /// plane through `base_pixel`.
    fn plane_pixel_polygons(
        &self,
        coords: &CoordinateSystem,
        base_pixel: &[f64],
    ) -> Result<PixelPolygons, ImageError> {
        let [x_axis, y_axis] = self.pixel_axes;
        let base_world = coords.to_world(base_pixel)?;
        self.polygons
            .iter()
            .map(|polygon| {
                polygon
                    .iter()
                    .map(|vertex| {
                        let mut world = base_world.clone();
                        world[x_axis] = vertex[0];
                        world[y_axis] = vertex[1];
                        let pixel = coords.to_pixel(&world)?;
                        let pixel = [pixel[x_axis], pixel[y_axis]];
                        if pixel.iter().all(|value| value.is_finite()) {
                            Ok(pixel)
                        } else {
                            Err(ImageError::InvalidMetadata(format!(
                                "polygon vertex {vertex:?} has no pixel position in this image"
                            )))
                        }
                    })
                    .collect()
            })
            .collect()
    }
}

impl<T: ImagePixel> PagedImage<T> {
    /// Reads the saved region `name` as a polygon region.
    ///
    /// Mirrors C++ `ImageInterface::getRegion` for the `WCPolygon` class and
    /// a `WCUnion` of `WCPolygon`s; any other class is an error that names
    /// it. See [`PolygonRegion::from_record`].
    pub fn get_polygon_region(&self, name: &str) -> Result<PolygonRegion, ImageError> {
        let record = self.get_region_record(name)?;
        PolygonRegion::from_record(name, &record, self.coordinates())
    }

    /// Saves `region` as the image region `name`, replacing any region of
    /// that name.
    ///
    /// Mirrors C++ `ImageInterface::defineRegion` with
    /// `RegionHandler::Regions`. Like other metadata changes, it reaches disk
    /// when the image is [saved](Self::save).
    pub fn put_polygon_region(
        &mut self,
        name: &str,
        region: &PolygonRegion,
    ) -> Result<(), ImageError> {
        let record = region.to_record(self.coordinates())?;
        self.put_region_record(name, &record)
    }

    /// Writes `region` as the mask `mask_name`, `true` inside the region,
    /// and optionally makes it the default mask.
    ///
    /// The mask pixels are written at once, as [`put_mask`](Self::put_mask)
    /// does; the default-mask choice reaches disk when the image is
    /// [saved](Self::save).
    pub fn put_region_mask(
        &mut self,
        mask_name: &str,
        region: &PolygonRegion,
        make_default: bool,
    ) -> Result<(), ImageError> {
        let mask = region.mask(self.coordinates(), self.shape())?;
        self.put_mask(mask_name, &mask)?;
        if make_default {
            self.set_default_mask(mask_name)?;
        }
        Ok(())
    }
}

impl<T: ImagePixel + StatsElement> PagedImage<T> {
    /// Computes statistics of the pixels `region` selects.
    ///
    /// Returns `None` when the region selects no finite pixel that the
    /// default mask allows. See [`RegionStatistics`] for which pixels count.
    /// The image is read one plane's bounding box at a time.
    pub fn region_statistics(
        &self,
        region: &PolygonRegion,
    ) -> Result<Option<RegionStatistics>, ImageError> {
        let [x_axis, y_axis] = region.pixel_axes;
        let mut values = Vec::new();
        region.visit_planes(self.coordinates(), self.shape(), |plane| {
            let data = self.get_slice(&plane.start, &plane.extent)?;
            let stride = vec![1; plane.extent.len()];
            let mask = self.get_mask_slice(&plane.start, &plane.extent, &stride)?;
            for (index, pixel) in data.indexed_iter() {
                if !plane.inside[[index[x_axis], index[y_axis]]] {
                    continue;
                }
                if mask.as_ref().is_some_and(|mask| !mask[index.slice()]) {
                    continue;
                }
                let value = pixel.to_f64_stats();
                if value.is_finite() {
                    values.push(value);
                }
            }
            Ok(())
        })?;
        Ok(statistics_of(values, self.units()))
    }
}

fn statistics_of(mut values: Vec<f64>, unit: &str) -> Option<RegionStatistics> {
    if values.is_empty() {
        return None;
    }
    let pixel_count = values.len();
    let count = pixel_count as f64;
    let sum = values.iter().sum::<f64>();
    let sum_sq = values.iter().map(|value| value * value).sum::<f64>();
    let mean = sum / count;
    let variance = values
        .iter()
        .map(|value| {
            let delta = value - mean;
            delta * delta
        })
        .sum::<f64>()
        / count;
    let min = values.iter().copied().fold(f64::INFINITY, f64::min);
    let max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    values.sort_by(f64::total_cmp);
    let median = (values[(pixel_count - 1) / 2] + values[pixel_count / 2]) / 2.0;
    Some(RegionStatistics {
        pixel_count,
        sum,
        mean,
        median,
        rms: (sum_sq / count).sqrt(),
        sigma: variance.sqrt(),
        min,
        max,
        unit: unit.to_string(),
    })
}

/// Finds the pixels inside `polygons` (pixel coordinates on the region axes)
/// within their bounding box, or `None` when the box misses the image.
fn select_plane(
    polygons: &[Vec<[f64; 2]>],
    shape: &[usize],
    [x_axis, y_axis]: [usize; 2],
) -> Option<PlaneSelection> {
    let (x_range, y_range) = (
        pixel_range(polygons, 0, shape[x_axis])?,
        pixel_range(polygons, 1, shape[y_axis])?,
    );
    let inside = Array2::from_shape_fn(
        (x_range.1 - x_range.0 + 1, y_range.1 - y_range.0 + 1),
        |(x, y)| {
            let centre = [(x_range.0 + x) as f64, (y_range.0 + y) as f64];
            polygons
                .iter()
                .any(|polygon| polygon_contains_pixel(polygon, centre))
        },
    );
    if !inside.iter().any(|&selected| selected) {
        return None;
    }
    let mut start = vec![0; shape.len()];
    let mut extent = vec![1; shape.len()];
    start[x_axis] = x_range.0;
    start[y_axis] = y_range.0;
    extent[x_axis] = inside.nrows();
    extent[y_axis] = inside.ncols();
    Some(PlaneSelection {
        start,
        extent,
        inside,
    })
}

/// The inclusive range of pixel indices on one region axis that the polygons
/// can touch, clipped to an axis of `len` pixels.
fn pixel_range(
    polygons: &[Vec<[f64; 2]>],
    coordinate: usize,
    len: usize,
) -> Option<(usize, usize)> {
    let values = polygons.iter().flatten().map(|vertex| vertex[coordinate]);
    let low = values.clone().fold(f64::INFINITY, f64::min).floor();
    let high = values.fold(f64::NEG_INFINITY, f64::max).ceil();
    if len == 0 || high < 0.0 || low > (len - 1) as f64 {
        return None;
    }
    Some((low.max(0.0) as usize, high.min((len - 1) as f64) as usize))
}

fn check_pixel_axes(pixel_axes: [usize; 2], n_axes: usize) -> Result<(), ImageError> {
    if pixel_axes[0] == pixel_axes[1] || pixel_axes.iter().any(|&axis| axis >= n_axes) {
        return Err(ImageError::InvalidMetadata(format!(
            "polygon region axes {pixel_axes:?} must be two different axes of a {n_axes}-axis coordinate system"
        )));
    }
    Ok(())
}

/// The world-axis units of `coords`, one per pixel axis.
fn world_axis_units(coords: &CoordinateSystem) -> Vec<String> {
    let mut units = Vec::with_capacity(coords.n_pixel_axes());
    for index in 0..coords.n_coordinates() {
        let coordinate = coords.coordinate(index);
        let coordinate_units = coordinate.axis_units();
        for axis in 0..coordinate.n_pixel_axes() {
            units.push(coordinate_units.get(axis).cloned().unwrap_or_default());
        }
    }
    units
}

#[cfg(test)]
mod tests;
