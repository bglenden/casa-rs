// SPDX-License-Identifier: LGPL-3.0-or-later

use crate::{CoordinateError, CoordinateModel, CoordinateSystem};

impl CoordinateSystem {
    /// Clear the transient mixed-coordinate search intervals, as on record restore.
    pub fn reset_direction_mix_ranges(&mut self) {
        for model in &mut self.coordinates {
            if let CoordinateModel::Direction(direction) = model {
                direction.reset_mix_ranges();
            }
        }
    }

    /// Transfer the transient search intervals after an in-place edit that
    /// preserved the coordinates and their creation order.
    pub fn retain_direction_mix_ranges(&mut self, previous: &Self) {
        for (model, old) in self.coordinates.iter_mut().zip(&previous.coordinates) {
            if let (CoordinateModel::Direction(direction), CoordinateModel::Direction(old)) =
                (model, old)
            {
                direction.retain_mix_ranges(old);
            }
        }
    }

    /// Update the sky intervals used by transforms with mixed pixel and
    /// world inputs. Directions with an unknown or removed axis use the
    /// corresponding reference pixel to determine the search interval.
    ///
    /// # Errors
    /// The image shape must have one length per public pixel axis. A
    /// direction projection can reject the image centre.
    pub fn set_direction_mix_shape(&mut self, shape: &[i64]) -> Result<(), CoordinateError> {
        let count = self.n_pixel_axes();
        if shape.len() != count {
            return Err(CoordinateError::DimensionMismatch {
                expected: count,
                got: shape.len(),
            });
        }
        let maps = self.maps.as_deref().filter(|maps| !maps.concatenated);
        let mut start = 0;
        for (index, model) in self.coordinates.iter_mut().enumerate() {
            let end = start + model.n_pixel_axes();
            if let CoordinateModel::Direction(direction) = model {
                let mut fixed = [None; 2];
                let local = if let Some(maps) = maps {
                    let placement = &maps.placements[index];
                    [0, 1].map(|axis| {
                        if let Some(&pixel) = placement.pixel.indices.get(axis) {
                            if let Ok(public) = usize::try_from(pixel) {
                                return shape[public];
                            }
                            if placement.world.indices[axis] >= 0 {
                                fixed[axis] = Some(placement.pixel.replacements[axis]);
                            }
                        }
                        -1
                    })
                } else if let Some(removed) = direction.removed_axis() {
                    let mut local = [-1; 2];
                    local[1 - removed.index] = shape[start];
                    local
                } else {
                    [shape[start], shape[start + 1]]
                };
                direction.set_mix_shape(local, fixed)?;
            }
            start = end;
        }
        Ok(())
    }

    /// Find the absolute pixel vector from a mix of absolute world and pixel
    /// values. A true entry in `world_axes` supplies the world value at the
    /// corresponding public axis index. A false entry supplies its pixel.
    /// Direction world values use radians.
    ///
    /// # Errors
    /// Requires equal pixel and world ranks and one input slot per axis. An
    /// input combination with no solution also returns an error.
    pub fn pixels_from_mixed(
        &self,
        world: &[f64],
        pixel: &[f64],
        world_axes: &[bool],
    ) -> Result<Vec<f64>, CoordinateError> {
        let count = self.n_world_axes();
        if self.n_pixel_axes() != count {
            return Err(CoordinateError::InvalidRecord(
                "Number of pixel and world axes differs".into(),
            ));
        }
        for length in [world.len(), pixel.len(), world_axes.len()] {
            if length != count {
                return Err(CoordinateError::DimensionMismatch {
                    expected: count,
                    got: length,
                });
            }
        }
        if world_axes.iter().all(|known| *known) {
            return self.to_pixel(world);
        }
        if world_axes.iter().all(|known| !*known) {
            self.to_world(pixel)?;
            return Ok(pixel.to_vec());
        }
        let mut result = vec![0.0; count];
        if let Some(maps) = self.nonconsecutive_maps() {
            for (model, placement) in self.coordinates.iter().zip(&maps.placements) {
                let hidden_world_is_world =
                    !matches!(model, CoordinateModel::Direction(_))
                        || placement.world.indices.iter().any(|&index| {
                            usize::try_from(index).is_ok_and(|axis| world_axes[axis])
                        });
                let local_world_axes = placement
                    .world
                    .indices
                    .iter()
                    .map(|&index| {
                        usize::try_from(index)
                            .map_or(hidden_world_is_world, |axis| world_axes[axis])
                    })
                    .collect::<Vec<_>>();
                let local_pixel_axes = placement
                    .pixel
                    .indices
                    .iter()
                    .zip(&local_world_axes)
                    .map(|(&index, &known_world)| {
                        usize::try_from(index).map_or(!known_world, |axis| !world_axes[axis])
                    })
                    .collect::<Vec<_>>();
                let hidden = placement.coordinate_world_replacements(model)?;
                let local_world = placement.world.gather(world, &hidden);
                let local_pixel = placement.pixel.gather(pixel, &placement.pixel.replacements);
                let converted = local_pixels(
                    model,
                    &local_world,
                    &local_pixel,
                    &local_world_axes,
                    &local_pixel_axes,
                )?;
                placement.pixel.scatter(&converted, &mut result);
            }
        } else {
            let mut start = 0;
            for model in &self.coordinates {
                let end = start + model.n_pixel_axes();
                let local = &world_axes[start..end];
                let pixels = local.iter().map(|known| !known).collect::<Vec<_>>();
                let values = local_pixels(
                    model,
                    &world[start..end],
                    &pixel[start..end],
                    local,
                    &pixels,
                )?;
                result[start..end].copy_from_slice(&values);
                start = end;
            }
        }
        Ok(result)
    }
}

fn local_pixels(
    model: &CoordinateModel,
    world: &[f64],
    pixel: &[f64],
    world_axes: &[bool],
    pixel_axes: &[bool],
) -> Result<Vec<f64>, CoordinateError> {
    let direction = matches!(model, CoordinateModel::Direction(_));
    let origin = if direction {
        "DirectionCoordinate::toMix"
    } else {
        "Coordinate::toMix"
    };
    for (&world, &pixel) in world_axes.iter().zip(pixel_axes) {
        if world && pixel {
            return Err(CoordinateError::ConversionFailed(format!(
                "{origin} - duplicate pixel/world axes"
            )));
        }
        if !world && !pixel {
            let noun = if direction { "coordinate" } else { "axis" };
            return Err(CoordinateError::ConversionFailed(format!(
                "{origin} - each {noun} must be either pixel or world"
            )));
        }
    }
    if world_axes.iter().all(|known| *known) {
        return model.to_pixel(world);
    }
    if pixel_axes.iter().all(|known| *known) {
        model.to_world(pixel)?;
        return Ok(pixel.to_vec());
    }
    if let CoordinateModel::Direction(direction) = model {
        let fixed_pixel = usize::from(world_axes[0]);
        let world = world.try_into().expect("two direction world axes");
        let pixel = pixel.try_into().expect("two direction pixel axes");
        return Ok(direction
            .pixels_from_mixed(world, pixel, fixed_pixel)?
            .to_vec());
    }
    // Casacore's default coordinate mixer converts from the supplied world
    // axes and reference values, then overlays the supplied pixel axes.
    let mut reference_world = model.reference_value();
    for (index, &known) in world_axes.iter().enumerate() {
        if known {
            reference_world[index] = world[index];
        }
    }
    let mut converted = model.to_pixel(&reference_world)?;
    let mut reference_pixel = model.reference_pixel();
    for (index, &known) in pixel_axes.iter().enumerate() {
        if known {
            converted[index] = pixel[index];
            reference_pixel[index] = pixel[index];
        }
    }
    model.to_world(&reference_pixel)?;
    Ok(converted)
}
