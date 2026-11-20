// SPDX-License-Identifier: LGPL-3.0-or-later
//! Geometry checks for CASA image concatenation.

use std::path::Path;

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, LinearCoordinate,
    SpectralCoordinate, StokesCoordinate, TabularCoordinate,
};

use casa_lattices::{TraversalIter, TraversalSpec};
use ndarray::Array2;

use crate::{
    ImageBeamSet, ImageError, ImageInfo, ImageInterface, ImagePixel, MutableImageInterface,
};

/// Group consecutive linear FITS axes as CASA's concatenation reader does.
/// Keep the pixel order, linear transforms, and observation information.
pub fn group_fits_linear_axes(input: &CoordinateSystem) -> Option<CoordinateSystem> {
    let mut output = CoordinateSystem::new().with_obs_info(input.obs_info().clone());
    let mut changed = false;
    let mut index = 0;
    while index < input.n_coordinates() {
        let first = index;
        let mut linear = Vec::new();
        while index < input.n_coordinates() {
            let CoordinateModel::Linear(axis) = input.coordinate(index) else {
                break;
            };
            linear.push(axis);
            index += 1;
        }
        if linear.len() < 2 {
            output.add_coordinate(input.coordinate(first).clone());
            index = first + 1;
            continue;
        }
        changed = true;
        let count = linear.iter().map(|axis| axis.n_pixel_axes()).sum();
        let mut transform = Array2::zeros((count, count));
        let mut start = 0;
        for axis in &linear {
            for ((row, column), value) in axis.pc_matrix().indexed_iter() {
                transform[[start + row, start + column]] = *value;
            }
            start += axis.n_pixel_axes();
        }
        let names = linear.iter().flat_map(|axis| axis.axis_names()).collect();
        let units = linear.iter().flat_map(|axis| axis.axis_units()).collect();
        output.add_coordinate(
            LinearCoordinate::new(count, names, units)
                .with_reference_value(
                    linear
                        .iter()
                        .flat_map(|axis| axis.reference_value())
                        .collect(),
                )
                .with_reference_pixel(
                    linear
                        .iter()
                        .flat_map(|axis| axis.reference_pixel())
                        .collect(),
                )
                .with_increment(linear.iter().flat_map(|axis| axis.increment()).collect())
                .with_pc_matrix(transform),
        );
    }
    changed.then_some(output)
}

/// An input image's geometry and diagnostic name.
pub struct ConcatInput<'a> {
    /// The filename supplied to the image opener.
    pub path: &'a Path,
    /// CASA pixel shape.
    pub shape: &'a [usize],
    /// The image's coordinate system.
    pub coordinates: &'a CoordinateSystem,
    /// Image metadata, including restoring beams.
    pub image_info: &'a ImageInfo,
    /// Brightness unit as stored in the image.
    pub brightness_unit: &'a str,
}

/// Validated geometry for combining image pixels in order.
pub struct ConcatenationPlan {
    /// The concatenation axis in pixel order.
    pub axis: usize,
    /// Indices of the inputs in the order used for the result.
    pub order: Vec<usize>,
    /// Combined image shape.
    pub shape: Vec<usize>,
    /// Coordinate system of the output.
    pub coordinates: CoordinateSystem,
    /// Whether the operation rebuilt a coordinate on the combined axis.
    pub coordinates_changed: bool,
    /// Image metadata from the first image and the combined restoring beams.
    pub image_info: ImageInfo,
    /// Diagnostics for accepted differences between source grids.
    pub warnings: Vec<String>,
}

struct Axis {
    coordinate: usize,
    kind: CoordinateType,
    name: String,
    unit: String,
    reference_pixel: f64,
    reference_value: f64,
    increment: f64,
}

fn axes(coordinates: &CoordinateSystem) -> Vec<Axis> {
    let mut output = Vec::new();
    for index in 0..coordinates.n_coordinates() {
        let coordinate = coordinates.coordinate(index);
        let kind = coordinate.coordinate_type();
        let names = coordinate.axis_names();
        let units = coordinate.axis_units();
        let increments = coordinate.increment();
        let pixels = coordinate.reference_pixel();
        let values = coordinate.reference_value();
        for i in 0..coordinate.n_pixel_axes() {
            output.push(Axis {
                coordinate: index,
                kind,
                name: names[i].clone(),
                unit: units[i].clone(),
                reference_pixel: pixels[i],
                reference_value: values[i],
                increment: increments[i],
            });
        }
    }
    output
}

fn world_at(
    input: &ConcatInput<'_>,
    axes: &[Axis],
    axis: usize,
    pixel: f64,
) -> Result<f64, ImageError> {
    let mut at: Vec<_> = axes.iter().map(|axis| axis.reference_pixel).collect();
    at[axis] = pixel;
    Ok(input.coordinates.to_world(&at)?[axis])
}

fn near(lhs: f64, rhs: f64) -> bool {
    lhs == rhs || (lhs - rhs).abs() <= 1e-13 * lhs.abs().max(rhs.abs())
}

fn mismatch(warnings: &mut Vec<String>, relax: bool, message: &str) -> Result<(), ImageError> {
    if !relax {
        return Err(ImageError::CasaSelection(message.into()));
    }
    if !warnings.iter().any(|warning| warning == message) {
        warnings.push(message.to_string());
    }
    Ok(())
}

fn resolve_axis(
    inputs: &[ConcatInput<'_>],
    bases: &[Vec<Axis>],
    axis: i64,
) -> Result<usize, ImageError> {
    let mut selected_axis = None;
    for (input, axes) in inputs.iter().zip(bases) {
        let resolved = if axis >= 0 {
            let axis = axis as usize;
            if axis >= input.shape.len() {
                return Err(ImageError::CasaSelection(format!(
                    "Specified zero-based value of axis exceeds number of dimensions in {}",
                    input.path.display()
                )));
            }
            axis
        } else {
            axes.iter()
                .position(|axis| axis.kind == CoordinateType::Spectral)
                .ok_or_else(|| {
                    ImageError::CasaSelection(format!(
                        "Image {} has no spectral axis",
                        input.path.display()
                    ))
                })?
        };
        if selected_axis
            .replace(resolved)
            .is_some_and(|previous| previous != resolved)
        {
            return Err(ImageError::CasaSelection(
                "Spectral axis numbers between images are not the same".into(),
            ));
        }
        if axes.len() != input.shape.len() {
            return Err(ImageError::CasaSelection(
                "Pixel axis has been removed for concatenation axis".into(),
            ));
        }
        if input.shape.contains(&0) {
            return Err(ImageError::InvalidMetadata(
                "concatenation needs nonempty axes".into(),
            ));
        }
    }
    selected_axis.ok_or_else(|| ImageError::InvalidMetadata("no concatenation axis".into()))
}

impl ConcatenationPlan {
    /// Check dimensions and coordinates, optionally reordering inputs.
    /// A negative axis selects the spectral axis of each image.
    ///
    /// # Errors
    ///
    /// An invalid axis, incompatible shapes, or grids that CASA rejects.
    pub fn new(
        inputs: &[ConcatInput<'_>],
        axis: i64,
        relax: bool,
        reorder: bool,
    ) -> Result<Self, ImageError> {
        if inputs.len() < 2 {
            return Err(ImageError::CasaSelection(
                "You must provide at least two images to concatentate".into(),
            ));
        }
        let bases: Vec<_> = inputs.iter().map(|input| axes(input.coordinates)).collect();
        let axis = resolve_axis(inputs, &bases, axis)?;
        let mut order: Vec<_> = (0..inputs.len()).collect();
        let increasing = bases[0][axis].increment > 0.0;
        if (!relax || reorder)
            && bases
                .iter()
                .any(|axes| (axes[axis].increment > 0.0) != increasing)
        {
            return Err(ImageError::CasaSelection(
                "Coordinate axes in different images with opposing increment signs is not permitted if relax=false or reorder=true".into(),
            ));
        }
        if reorder {
            let locations = inputs
                .iter()
                .zip(&bases)
                .map(|(input, axes)| {
                    let pixel = if increasing {
                        0.0
                    } else {
                        (input.shape[axis] - 1) as f64
                    };
                    world_at(input, axes, axis, pixel)
                })
                .collect::<Result<Vec<_>, _>>()?;
            order.sort_by(|&left, &right| {
                let order = locations[left].total_cmp(&locations[right]);
                if increasing { order } else { order.reverse() }
            });
        }
        let first = &inputs[order[0]];
        let reference = &bases[order[0]];
        let mut output = Self {
            axis,
            order,
            shape: first.shape.to_vec(),
            coordinates: first.coordinates.clone(),
            coordinates_changed: false,
            image_info: first.image_info.clone(),
            warnings: Vec::new(),
        };
        let mut previous = output.order[0];
        let mut contiguous = true;
        let selected = &reference[axis];
        let mut stokes = match first.coordinates.coordinate(selected.coordinate) {
            CoordinateModel::Stokes(coordinate) => coordinate.stokes().to_vec(),
            _ => Vec::new(),
        };
        for &index in &output.order[1..] {
            let input = &inputs[index];
            let current = &bases[index];
            if input.shape.len() != first.shape.len() {
                return Err(ImageError::CasaSelection(
                    "Images do not have the same number of dimensions".into(),
                ));
            }
            if first
                .shape
                .iter()
                .zip(input.shape)
                .enumerate()
                .any(|(i, (a, b))| i != axis && a != b)
            {
                return Err(ImageError::CasaSelection(
                    "Lattice shapes are not the same on non-concatenation axes".into(),
                ));
            }
            if first.coordinates.n_coordinates() != input.coordinates.n_coordinates() {
                return Err(ImageError::CasaSelection(
                    "Images have inconsistent numbers of coordinates".into(),
                ));
            }
            if current[axis].kind != selected.kind {
                return Err(ImageError::CasaSelection(
                    "Coordinate types for concatenation axis are inconsistent".into(),
                ));
            }
            output.image_info.beam_set = combine_beams(
                &output.image_info.beam_set,
                &input.image_info.beam_set,
                selected.kind,
                [output.shape[axis], input.shape[axis]],
                relax,
                &mut output.warnings,
            )?;
            if reference.iter().zip(current).any(|(a, b)| a.name != b.name) {
                mismatch(&mut output.warnings, relax, "Image axis names differ")?;
            }
            if reference.iter().zip(current).any(|(a, b)| a.unit != b.unit) {
                mismatch(&mut output.warnings, relax, "Image axis units differ")?;
            }
            if input.brightness_unit != first.brightness_unit {
                mismatch(
                    &mut output.warnings,
                    true,
                    &format!(
                        "Image units differ. Image units of the first image ({}) will be used for the output image",
                        first.brightness_unit
                    ),
                )?;
            }
            if contiguous {
                contiguous = if let CoordinateModel::Stokes(coordinate) =
                    input.coordinates.coordinate(current[axis].coordinate)
                {
                    if coordinate
                        .stokes()
                        .iter()
                        .any(|label| stokes.contains(label))
                    {
                        false
                    } else {
                        stokes.extend_from_slice(coordinate.stokes());
                        true
                    }
                } else {
                    let prior = &inputs[previous];
                    let last = world_at(
                        prior,
                        &bases[previous],
                        axis,
                        (prior.shape[axis] - 1) as f64,
                    )?;
                    let before = world_at(input, current, axis, -1.0)?;
                    (last - before).abs() <= 0.01 * bases[previous][axis].increment.abs()
                };
                if !contiguous {
                    mismatch(
                        &mut output.warnings,
                        relax,
                        "Images are not contiguous along the concatenation axis",
                    )?;
                }
            }
            for (nonconcat, (old, new)) in reference.iter().zip(current).enumerate() {
                if nonconcat == axis {
                    continue;
                }
                for (matches, property) in [
                    (
                        near(old.reference_pixel, new.reference_pixel),
                        "reference pixels",
                    ),
                    (
                        near(old.reference_value, new.reference_value),
                        "reference values",
                    ),
                    (near(old.increment, new.increment), "increments"),
                ] {
                    if !matches {
                        mismatch(
                            &mut output.warnings,
                            relax,
                            &format!(
                                "Image {property} are different on non-concatenation axis {}",
                                nonconcat + 1
                            ),
                        )?;
                    }
                }
            }
            output.shape[axis] = output.shape[axis]
                .checked_add(input.shape[axis])
                .ok_or_else(|| {
                    ImageError::InvalidMetadata("concatenated axis length overflows".into())
                })?;
            previous = index;
        }
        if contiguous && selected.kind == CoordinateType::Stokes {
            output
                .coordinates
                .replace_coordinate(selected.coordinate, StokesCoordinate::new(stokes));
            output.coordinates_changed = true;
        } else if !contiguous {
            output.rebuild_axis(inputs, &bases, selected)?;
        }
        Ok(output)
    }

    fn rebuild_axis(
        &mut self,
        inputs: &[ConcatInput<'_>],
        bases: &[Vec<Axis>],
        axis: &Axis,
    ) -> Result<(), ImageError> {
        let original = self.coordinates.coordinate(axis.coordinate);
        if original.n_pixel_axes() != 1 || original.n_world_axes() != 1 {
            self.warnings
                .push("CoordinateSystem set to that of first image".into());
            return Ok(());
        }
        let mut world = Vec::with_capacity(self.shape[self.axis]);
        for &index in &self.order {
            for pixel in 0..inputs[index].shape[self.axis] {
                world.push(world_at(
                    &inputs[index],
                    &bases[index],
                    self.axis,
                    pixel as f64,
                )?);
            }
        }
        let inc = world[1] - world[0];
        if inc == 0.0
            || !world
                .windows(2)
                .all(|pair| ((pair[1] - pair[0]) * inc) > 0.0)
        {
            self.warnings
                .push("CoordinateSystem set to that of first image".into());
            return Ok(());
        }
        let pixels = (0..world.len()).map(|i| i as f64).collect();
        let coordinate = if let CoordinateModel::Spectral(spectral) = original {
            CoordinateModel::from(SpectralCoordinate::from_tabular(
                spectral.frequency_ref(),
                pixels,
                world.clone(),
                world[0],
                inc,
                0.0,
                spectral.rest_frequency(),
            )?)
        } else {
            CoordinateModel::from(TabularCoordinate::new(
                pixels,
                world,
                axis.name.clone(),
                axis.unit.clone(),
            ))
        };
        self.coordinates
            .replace_coordinate(axis.coordinate, coordinate);
        self.coordinates_changed = true;
        Ok(())
    }
}

fn combine_beams(
    original: &ImageBeamSet,
    next: &ImageBeamSet,
    kind: CoordinateType,
    lengths: [usize; 2],
    relax: bool,
    warnings: &mut Vec<String>,
) -> Result<ImageBeamSet, ImageError> {
    if original.is_empty() != next.is_empty() {
        mismatch(
            warnings,
            relax,
            "One image does not have a beam while another does",
        )?;
        return Ok(ImageBeamSet::empty());
    }
    if original.is_empty() {
        return Ok(ImageBeamSet::empty());
    }
    let [channels, stokes] = [
        [original.n_channels(), next.n_channels()],
        [original.n_stokes(), next.n_stokes()],
    ];
    let expand = |sizes: [usize; 2]| sizes[0] == sizes[1] || sizes.contains(&1);
    let shape_error = || ImageError::InvalidMetadata("incompatible restoring beam grid".into());
    let equivalent = original.equivalent(next);
    if kind == CoordinateType::Spectral {
        if !expand(stokes)
            || !channels
                .into_iter()
                .zip(lengths)
                .all(|(count, len)| count == 1 || count == len)
        {
            return Err(shape_error());
        }
        if channels[0] == 1 && equivalent {
            return Ok(original.clone());
        }
        let planes = stokes[0].max(stokes[1]);
        let grid = [(original, lengths[0]), (next, lengths[1])]
            .into_iter()
            .flat_map(|(set, count)| {
                (0..count).map(move |channel| {
                    (0..planes).map(|plane| *set.beam(channel, plane)).collect()
                })
            })
            .collect();
        return Ok(ImageBeamSet::from_grid(grid));
    }
    if kind == CoordinateType::Stokes {
        if !expand(channels)
            || !stokes
                .into_iter()
                .zip(lengths)
                .all(|(count, len)| count == 1 || count == len)
        {
            return Err(shape_error());
        }
        if stokes[0] == 1 && equivalent {
            return Ok(original.clone());
        }
        let grid = (0..channels[0].max(channels[1]))
            .map(|channel| {
                [(original, lengths[0]), (next, lengths[1])]
                    .into_iter()
                    .flat_map(|(set, count)| (0..count).map(move |plane| *set.beam(channel, plane)))
                    .collect()
            })
            .collect();
        return Ok(ImageBeamSet::from_grid(grid));
    }
    if !expand(channels) || !expand(stokes) {
        return Err(shape_error());
    }
    let max_channels = channels[0].max(channels[1]);
    let max_stokes = stokes[0].max(stokes[1]);
    for (index, set) in [original, next].into_iter().enumerate() {
        if channels[index] == max_channels && stokes[index] == max_stokes {
            if !equivalent {
                mismatch(warnings, relax, "Beams of images are not equivalent")?;
            }
            return Ok(set.clone());
        }
    }
    mismatch(
        warnings,
        relax,
        "One beam set varies in frequency, the other in polarization",
    )?;
    Ok(if channels[0] == max_channels {
        original
    } else {
        next
    }
    .clone())
}

/// Copy one input at `offset` along the concatenation axis in bounded tiles.
/// The output mask receives true where an input has no mask.
///
/// # Errors
///
/// An incompatible destination or a read or write failure.
pub fn copy_concatenated_input<T: ImagePixel>(
    input: &dyn ImageInterface<T>,
    output: &mut dyn MutableImageInterface<T>,
    axis: usize,
    offset: usize,
    mask: Option<&str>,
) -> Result<(), ImageError> {
    if axis >= input.ndim() || input.ndim() != output.ndim() {
        return Err(ImageError::ShapeMismatch {
            expected: output.shape().to_vec(),
            got: input.shape().to_vec(),
        });
    }
    let steps = vec![1; input.ndim()];
    for chunk in TraversalIter::new(input, TraversalSpec::tiles()) {
        let chunk = chunk?;
        let mut destination = chunk.cursor.position.clone();
        destination[axis] += offset;
        if let Some(name) = mask {
            let valid = input
                .get_mask_slice(&chunk.cursor.position, chunk.data.shape(), &steps)?
                .unwrap_or_else(|| ndarray::ArrayD::from_elem(chunk.data.raw_dim(), true));
            output.put_mask_slice(name, &valid, &destination)?;
        }
        output.put_slice(&chunk.data, &destination)?;
    }
    Ok(())
}
