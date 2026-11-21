// SPDX-License-Identifier: LGPL-3.0-or-later

mod geometry;
mod statistic;

use casa_coordinates::{CoordinateModel, CoordinateType};
use casa_lattices::SamplePoint;
use casa_types::quanta::Quantity;
use casa_types::{ArrayD, RecordValue};
use ndarray::{Array2, Axis, Ix2, IxDyn};

use crate::regrid::{Plane, RegridMethod};
use crate::{ImageError, ImageInterface, MutableImageInterface};

use super::super::lel_mask::PreparedMask;
use super::read_window::PixelSections;
use super::{StridedSection, SubimageOptions, SubimagePlan, SubimageRegion};
use geometry::Window;
use statistic::LocalStatistic;

/// Options for a local-statistic image along the direction plane.
pub struct DeviationOptions<'a> {
    /// Region of the source to transform, retaining all pixel axes.
    pub region: Option<SubimageRegion<'a>>,
    /// Additional validity expression on the source.
    pub mask: Option<&'a str>,
    /// Permit broadcasting compatible mask axes.
    pub stretch: bool,
    /// Positive interval between statistic samples on each direction axis.
    pub grid: [usize; 2],
    /// Anchor pixel in the parent image, or its rounded reference pixel.
    pub anchor: Option<[i64; 2]>,
    /// Box width, or circle diameter when the height is absent.
    pub x_length: Quantity,
    /// Box height, or no value for circular sampling.
    pub y_length: Option<Quantity>,
    /// Interpolate between sampled grid points by this method.
    pub interpolation: RegridMethod,
    /// CASA statistic name or supported prefix.
    pub statistic: &'a str,
    /// Classical or Chauvenet sample selection.
    pub algorithm: &'a str,
    /// Chauvenet cutoff, or a negative value for automatic selection.
    pub zscore: f64,
    /// Chauvenet iteration limit, or a negative value for automatic selection.
    pub maxiter: i64,
}

/// Selected geometry and grid for `image.deviation`.
pub struct ImageDeviation<'a> {
    section: SubimagePlan,
    axes: [usize; 2],
    grid: Grid,
    window: Window,
    statistic: LocalStatistic,
    options: DeviationOptions<'a>,
}

fn invalid(message: impl Into<String>) -> ImageError {
    ImageError::CasaSelection(message.into())
}

impl<'a> ImageDeviation<'a> {
    /// Resolve the input selection, direction-plane geometry and sampling grid.
    ///
    /// # Errors
    /// The source coordinates, image selection, statistic or geometry is invalid.
    pub fn new(
        image: &dyn ImageInterface<f32>,
        coordinate_record: RecordValue,
        beams: &RecordValue,
        options: DeviationOptions<'a>,
    ) -> Result<Self, ImageError> {
        let coordinates = image.coordinates();
        let sky_index = coordinates
            .find_coordinate(CoordinateType::Direction)
            .ok_or_else(|| invalid("Coordinate system has no direction coordinate"))?;
        let [Some(x), Some(y)] = coordinates
            .coordinate_pixel_axes(sky_index)
            .and_then(|axes| axes.try_into().ok())
            .ok_or_else(|| invalid("A direction coordinate with two pixel axes is required"))?
        else {
            return Err(invalid(
                "A direction coordinate with two pixel axes is required",
            ));
        };
        let axes = [x, y];
        let mut anchor = vec![0.; image.ndim()];
        let reference = coordinates.reference_pixel();
        for (entry, axis) in axes.into_iter().enumerate() {
            anchor[axis] = options
                .anchor
                .map_or_else(|| reference[axis].round(), |point| point[entry] as f64);
        }
        let world_anchor = coordinates.to_world(&anchor)?;
        let section = SubimagePlan::new_with_beams(
            image,
            Some(coordinate_record),
            SubimageOptions {
                region: options.region,
                dropdeg: false,
                keepaxes: &[],
            },
            Some(beams),
        )?;
        let grid = Grid::new(options.grid, axes.map(|axis| section.shape[axis]), {
            let local = section.coordinates.to_pixel(&world_anchor)?;
            axes.map(|axis| local[axis].round_ties_even() as i64)
        })?;
        let CoordinateModel::Direction(direction) = section.coordinates.coordinate(sky_index)
        else {
            return Err(invalid("Coordinate system has no direction coordinate"));
        };
        let window = Window::new(
            direction,
            grid.shape,
            &options.x_length,
            options.y_length.as_ref(),
        )?;
        let statistic = LocalStatistic::new(
            options.statistic,
            options.algorithm,
            options.zscore,
            options.maxiter,
        )?;
        Ok(Self {
            section,
            axes,
            grid,
            window,
            statistic,
            options,
        })
    }

    /// Output image geometry and selected coordinate representation.
    pub fn section(&self) -> &SubimagePlan {
        &self.section
    }

    /// Fill a preallocated output with the local statistic and its validity mask.
    ///
    /// # Errors
    /// The mask, input or output storage fails, or an unmasked input cannot be
    /// interpolated on the requested grid.
    pub fn copy_to(
        &self,
        image: &dyn ImageInterface<f32>,
        output: &mut dyn MutableImageInterface<f32>,
    ) -> Result<(), ImageError> {
        self.section.copy_metadata(image, output)?;
        let selection = &self.section.selection;
        let unit = vec![1; image.ndim()];
        let section = StridedSection {
            start: &selection.start,
            shape: &self.section.shape,
            stride: selection.stride.as_deref().unwrap_or(&unit),
        };
        let reader = PixelSections {
            image,
            selection,
            mask: PreparedMask::new(
                self.options.mask,
                image.shape(),
                image.coordinates(),
                self.options.stretch,
            )?,
        };
        let mut cursor = vec![1; image.ndim()];
        for axis in self.axes {
            cursor[axis] = 32;
        }
        let mut sample = Vec::new();
        let mut source_masked = false;
        let mut interpolation_failed = false;
        let [x, y] = self.axes;
        for tile in section.cursors(cursor) {
            let tile = tile?;
            let first = self.axes.map(|axis| tile.position[axis]);
            let last = self
                .axes
                .map(|axis| tile.position[axis] + tile.shape[axis] - 1);
            let input = TileInput {
                reader: &reader,
                section: &section,
                axes: self.axes,
                plane: &tile.position,
            };
            let selected = input.read(first, last.map(|index| index + 1))?;
            let g_from = self
                .grid
                .index(first)
                .map(|index| ((index.floor() as isize) - 1).max(0) as usize);
            let g_last = self.grid.index(last);
            let g_to: [usize; 2] = std::array::from_fn(|i| {
                ((g_last[i].floor() as isize + 3).max(1) as usize).min(self.grid.count[i])
            });
            let grid_shape = std::array::from_fn(|i| g_to[i] - g_from[i]);
            let (lower, upper) = self.window.bounds(
                self.grid.point(g_from),
                self.grid.point(g_to.map(|index| index - 1)),
                self.grid.shape,
            );
            let cache_shape = self.axes.map(|axis| {
                let i = usize::from(axis == y);
                (upper[i] - lower[i] - 1) * section.stride[axis] + 1
            });
            let halo = if lower == selected.lower && upper == selected.upper() {
                None
            } else if cache_shape[0].saturating_mul(cache_shape[1]) <= 128 * 1024 {
                Some(input.read(lower, upper)?)
            } else {
                None
            };
            let cache = halo.as_ref().unwrap_or(&selected);
            let mut values = Vec::with_capacity(grid_shape[0] * grid_shape[1]);
            let mut available = Vec::with_capacity(values.capacity());
            for gy in g_from[1]..g_to[1] {
                for gx in g_from[0]..g_to[0] {
                    let center = self.grid.point([gx, gy]);
                    let value = self.stat_at(&input, cache, center, &mut sample)?;
                    values.push(f64::from(value.unwrap_or(0.)));
                    available.push(value.is_some());
                }
            }
            let grid = Plane {
                data: &values,
                mask: Some(&available),
                shape: grid_shape,
            };
            let mut values = ArrayD::zeros(IxDyn(&tile.shape));
            let mut valid = ArrayD::from_elem(IxDyn(&tile.shape), true);
            for ((index, value), good) in values.indexed_iter_mut().zip(valid.iter_mut()) {
                let position = [first[0] + index[x], first[1] + index[y]];
                let on_grid = (0..2).all(|axis| {
                    position[axis] >= self.grid.first[axis]
                        && (position[axis] - self.grid.first[axis])
                            .is_multiple_of(self.grid.step[axis])
                });
                let at = self.grid.index(position);
                let at: [f64; 2] = std::array::from_fn(|i| at[i] - g_from[i] as f64);
                let computed = if on_grid {
                    let i = at[0] as usize + at[1] as usize * grid_shape[0];
                    available[i].then(|| grid.data[i])
                } else {
                    grid.sample(self.options.interpolation, at)
                };
                let inherited = selected.get(position).is_some();
                source_masked |= !inherited;
                interpolation_failed |= !on_grid && computed.is_none();
                *good = inherited && computed.is_some();
                *value = computed.unwrap_or(0.) as f32;
            }
            if valid.iter().any(|&entry| !entry) {
                if output.default_mask_name().is_none() {
                    output.create_filled_mask("mask0", true)?;
                    output.set_default_mask("mask0")?;
                }
                output.put_mask_slice("mask0", &valid, &tile.position)?;
            }
            output.put_slice(&values, &tile.position)?;
        }
        if interpolation_failed && !source_masked {
            return Err(invalid(
                "Logic error: bad interpolation but there is no mask to set",
            ));
        }
        Ok(())
    }

    fn stat_at(
        &self,
        input: &TileInput<'_>,
        cache: &PlaneInput,
        center: [usize; 2],
        sample: &mut Vec<SamplePoint>,
    ) -> Result<Option<f32>, ImageError> {
        let (lower, upper) = self.window.bounds(center, center, self.grid.shape);
        sample.clear();
        let append = |pixels: &PlaneInput, sample: &mut Vec<SamplePoint>| {
            let limit = pixels.upper();
            for y in lower[1].max(pixels.lower[1])..upper[1].min(limit[1]) {
                for x in lower[0].max(pixels.lower[0])..upper[0].min(limit[0]) {
                    if self.window.contains(center, [x, y])
                        && let Some(value) = pixels.get([x, y])
                    {
                        sample.push(SamplePoint {
                            value: f64::from(value),
                            position: sample.len() as u64,
                        });
                    }
                }
            }
        };
        if cache.covers(lower, upper) {
            if cache.get(center).is_none() {
                return Ok(None);
            }
            append(cache, sample);
        } else {
            let peak = input.read(center, center.map(|i| i + 1))?;
            if peak.get(center).is_none() {
                return Ok(None);
            }
            for y in (lower[1]..upper[1]).step_by(64) {
                for x in (lower[0]..upper[0]).step_by(128) {
                    let end = [(x + 128).min(upper[0]), (y + 64).min(upper[1])];
                    append(&input.read([x, y], end)?, sample);
                }
            }
        }
        self.statistic.evaluate(sample)
    }
}

struct Grid {
    first: [usize; 2],
    step: [usize; 2],
    count: [usize; 2],
    shape: [usize; 2],
}

impl Grid {
    fn new(step: [usize; 2], shape: [usize; 2], anchor: [i64; 2]) -> Result<Self, ImageError> {
        if step
            .iter()
            .any(|&interval| interval == 0 || interval > i64::MAX as usize)
        {
            return Err(invalid("Both grid value(s) must be positive"));
        }
        let first = std::array::from_fn(|i| anchor[i].rem_euclid(step[i] as i64) as usize);
        if (0..2).any(|i| first[i] >= shape[i]) {
            return Err(invalid(
                "The grid contains no sample points in the selected image",
            ));
        }
        let count = std::array::from_fn(|i| (shape[i] - first[i]).div_ceil(step[i]));
        Ok(Self {
            first,
            step,
            count,
            shape,
        })
    }

    fn index(&self, point: [usize; 2]) -> [f64; 2] {
        std::array::from_fn(|i| (point[i] as f64 - self.first[i] as f64) / self.step[i] as f64)
    }

    fn point(&self, index: [usize; 2]) -> [usize; 2] {
        std::array::from_fn(|i| self.first[i] + index[i] * self.step[i])
    }
}

struct TileInput<'a> {
    reader: &'a PixelSections<'a, f32>,
    section: &'a StridedSection<'a>,
    axes: [usize; 2],
    plane: &'a [usize],
}

impl TileInput<'_> {
    fn read(&self, lower: [usize; 2], upper: [usize; 2]) -> Result<PlaneInput, ImageError> {
        let mut position = self.plane.to_vec();
        let mut shape = vec![1; position.len()];
        for (i, axis) in self.axes.into_iter().enumerate() {
            position[axis] = lower[i];
            shape[axis] = upper[i] - lower[i];
        }
        let (pixels, valid) = self.reader.read(
            &self.section.source_start(&position),
            &shape,
            self.section.stride,
        )?;
        Ok(PlaneInput {
            lower,
            pixels: direction_plane(pixels, self.axes),
            valid: valid.map(|mask| direction_plane(mask, self.axes)),
        })
    }
}

fn direction_plane<T>(mut data: ArrayD<T>, axes: [usize; 2]) -> Array2<T> {
    for i in (0..data.ndim()).rev().filter(|i| !axes.contains(i)) {
        data = data.index_axis_move(Axis(i), 0);
    }
    let data = data
        .into_dimensionality::<Ix2>()
        .expect("single direction plane");
    if axes[0] < axes[1] {
        data
    } else {
        data.reversed_axes()
    }
}

struct PlaneInput {
    lower: [usize; 2],
    pixels: Array2<f32>,
    valid: Option<Array2<bool>>,
}

impl PlaneInput {
    fn upper(&self) -> [usize; 2] {
        std::array::from_fn(|i| self.lower[i] + self.pixels.shape()[i])
    }

    fn covers(&self, lower: [usize; 2], upper: [usize; 2]) -> bool {
        let limit = self.upper();
        (0..2).all(|i| self.lower[i] <= lower[i] && upper[i] <= limit[i])
    }

    fn get(&self, at: [usize; 2]) -> Option<f32> {
        let index = [at[0] - self.lower[0], at[1] - self.lower[1]];
        self.valid
            .as_ref()
            .is_none_or(|mask| mask[index])
            .then(|| self.pixels[index])
    }
}
