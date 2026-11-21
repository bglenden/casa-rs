// SPDX-License-Identifier: LGPL-3.0-or-later
//! Local coordinate axes and their placement in each public vector.

use casa_types::quanta::{Quantity, Unit};
use casa_types::{ArrayValue, RecordValue, Value};
use ndarray::{Array2, ArrayView2, Axis, ShapeBuilder};

use crate::coordinate::CoordinateModel;
use crate::error::CoordinateError;
use crate::record_utils::{get_required_vec_f64, get_required_vec_i32};

#[derive(Debug, Clone)]
pub(super) struct AxisMap {
    pub(super) indices: Vec<i32>,
    replacements: Vec<f64>,
}

impl AxisMap {
    fn read(
        record: &RecordValue,
        index: usize,
        kind: &str,
        count: usize,
        nominal: impl FnOnce() -> Vec<f64>,
    ) -> Result<Self, CoordinateError> {
        let map_key = format!("{kind}map{index}");
        let indices = get_required_vec_i32(record, &map_key)?;
        if indices.len() != count {
            return Err(CoordinateError::InvalidRecord(format!(
                "{map_key} has {} entries for {} local axes",
                indices.len(),
                count
            )));
        }
        let key = format!("{kind}replace{index}");
        let replacements = if record.get(&key).is_some() || indices.iter().any(|&axis| axis < 0) {
            get_required_vec_f64(record, &key)?
        } else {
            nominal()
        };
        // Casacore preserves even surplus or short unused vectors. Hidden
        // axes require a complete vector because transforms read its values.
        if indices.iter().any(|&axis| axis < 0)
            && (replacements.len() != indices.len()
                || indices
                    .iter()
                    .zip(&replacements)
                    .any(|(&axis, value)| axis < 0 && !value.is_finite()))
        {
            return Err(CoordinateError::InvalidRecord(format!(
                "invalid {key} for {map_key}"
            )));
        }
        Ok(Self {
            indices,
            replacements,
        })
    }

    fn write(&self, record: &mut RecordValue, index: usize, kind: &str) {
        record.upsert(
            format!("{kind}map{index}"),
            Value::Array(ArrayValue::from_i32_vec(self.indices.clone())),
        );
        record.upsert(
            format!("{kind}replace{index}"),
            Value::Array(ArrayValue::from_f64_vec(self.replacements.clone())),
        );
    }

    fn gather(
        &self,
        mut value: impl FnMut(usize) -> f64,
        replacements: &[f64],
    ) -> impl Iterator<Item = f64> {
        self.indices.iter().enumerate().map(move |(local, &axis)| {
            usize::try_from(axis)
                .ok()
                .map_or_else(|| replacements[local], &mut value)
        })
    }

    fn scatter(&self, local: impl IntoIterator<Item = f64>, mut assign: impl FnMut(usize, f64)) {
        for (&axis, value) in self.indices.iter().zip(local) {
            if let Ok(axis) = usize::try_from(axis) {
                assign(axis, value);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct Placement {
    pub(super) pixel: AxisMap,
    pub(super) world: AxisMap,
    // Same values as `world.replacements`, in the model's computational units.
    model_world: Vec<f64>,
}

impl Placement {
    pub(super) fn nominal(model: &CoordinateModel, pixel: usize, world: usize) -> Self {
        let np = model.n_pixel_axes();
        let nw = model.n_world_axes();
        let pixel = AxisMap {
            indices: (pixel..pixel + np).map(|axis| axis as i32).collect(),
            replacements: vec![0.0; np],
        };
        let replacements = model
            .to_world(&pixel.replacements)
            .unwrap_or_else(|_| vec![0.0; nw]);
        let world = AxisMap {
            indices: (world..world + nw).map(|axis| axis as i32).collect(),
            replacements: to_record_units(model, replacements.clone()),
        };
        Self {
            pixel,
            world,
            model_world: replacements,
        }
    }

    pub(super) fn read(
        record: &RecordValue,
        index: usize,
        model: &CoordinateModel,
    ) -> Result<Self, CoordinateError> {
        let pixel = AxisMap::read(record, index, "pixel", model.n_pixel_axes(), || {
            vec![0.0; model.n_pixel_axes()]
        })?;
        let world = AxisMap::read(record, index, "world", model.n_world_axes(), || {
            to_record_units(
                model,
                model
                    .to_world(&vec![0.0; model.n_pixel_axes()])
                    .unwrap_or_else(|_| vec![0.0; model.n_world_axes()]),
            )
        })?;
        let model_world = to_model_units(model, &world)?;
        Ok(Self {
            pixel,
            world,
            model_world,
        })
    }

    pub(super) fn write(&self, record: &mut RecordValue, index: usize) {
        self.pixel.write(record, index, "pixel");
        self.world.write(record, index, "world");
    }

    pub(super) fn replace_model(&mut self, old: &CoordinateModel, new: &CoordinateModel) {
        let old_units = record_units(old);
        let new_units = record_units(new);
        let convert = || {
            self.world
                .replacements
                .iter()
                .zip(&old_units)
                .zip(&new_units)
                .map(|((&value, old), new)| convert_units(value, old, new))
                .collect::<Option<Vec<_>>>()
        };
        self.world.replacements = (old.coordinate_type() == new.coordinate_type())
            .then(convert)
            .flatten()
            .unwrap_or_else(|| {
                let reference = to_record_units(new, new.reference_value());
                self.world
                    .indices
                    .iter()
                    .zip(reference)
                    .map(|(&axis, value)| if axis < 0 { value } else { 0.0 })
                    .collect()
            });
        self.model_world = to_model_units(new, &self.world).expect("recognized coordinate units");
    }

    pub(super) fn to_world(
        &self,
        model: &CoordinateModel,
        pixel: &[f64],
        world: &mut [f64],
    ) -> Result<(), CoordinateError> {
        let input: Vec<_> = self
            .pixel
            .gather(|axis| pixel[axis], &self.pixel.replacements)
            .collect();
        self.world
            .scatter(model.to_world(&input)?, |axis, value| world[axis] = value);
        Ok(())
    }

    pub(super) fn to_pixel(
        &self,
        model: &CoordinateModel,
        world: &[f64],
        pixel: &mut [f64],
        undefined: bool,
    ) -> Result<(), CoordinateError> {
        let input: Vec<_> = self
            .world
            .gather(|axis| world[axis], &self.model_world)
            .collect();
        match model.to_pixel(&input) {
            Ok(output) => self
                .pixel
                .scatter(output, |axis, value| pixel[axis] = value),
            Err(CoordinateError::SingularTransform(_)) if undefined => self
                .pixel
                .scatter(std::iter::repeat(f64::NAN), |axis, value| {
                    pixel[axis] = value
                }),
            Err(error) => return Err(error),
        }
        Ok(())
    }

    pub(super) fn to_pixel_columns(
        &self,
        model: &CoordinateModel,
        world: ArrayView2<'_, f64>,
        pixels: &mut Array2<f64>,
        undefined: bool,
    ) -> Result<(), CoordinateError> {
        let mut local_world = Array2::zeros((self.world.indices.len(), world.ncols()).f());
        for (input, mut local) in world
            .axis_iter(Axis(1))
            .zip(local_world.axis_iter_mut(Axis(1)))
        {
            for (value, target) in self
                .world
                .gather(|axis| input[axis], &self.model_world)
                .zip(local.iter_mut())
            {
                *target = value;
            }
        }
        let mut local_pixels = Array2::zeros((self.pixel.indices.len(), world.ncols()).f());
        if let CoordinateModel::Linear(linear) = model {
            match linear.to_pixel_columns(local_world.view(), local_pixels.view_mut()) {
                Ok(()) => (),
                Err(CoordinateError::SingularTransform(_)) if undefined => {
                    local_pixels.fill(f64::NAN)
                }
                Err(error) => return Err(error),
            }
        } else {
            for (input, mut output) in local_world
                .axis_iter(Axis(1))
                .zip(local_pixels.axis_iter_mut(Axis(1)))
            {
                match model.to_pixel(input.as_slice().expect("column order")) {
                    Ok(values) => output
                        .iter_mut()
                        .zip(values)
                        .for_each(|(target, value)| *target = value),
                    Err(CoordinateError::SingularTransform(_)) if undefined => {
                        output.fill(f64::NAN)
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        for (local, mut output) in local_pixels
            .axis_iter(Axis(1))
            .zip(pixels.axis_iter_mut(Axis(1)))
        {
            self.pixel
                .scatter(local.iter().copied(), |axis, value| output[axis] = value);
        }
        Ok(())
    }
}

pub(super) fn validate_numbering(
    axes: impl Iterator<Item = i32>,
    kind: &str,
) -> Result<usize, CoordinateError> {
    let mut visible: Vec<_> = axes.filter(|&axis| axis >= 0).collect();
    visible.sort_unstable();
    if visible.iter().copied().eq(0..visible.len() as i32) {
        Ok(visible.len())
    } else {
        Err(CoordinateError::InvalidRecord(format!(
            "{kind} maps do not number their visible axes once each from 0: {visible:?}"
        )))
    }
}

fn record_units(model: &CoordinateModel) -> Vec<String> {
    match model {
        CoordinateModel::Direction(direction) => direction.world_units().to_vec(),
        _ => model.axis_units(),
    }
}

fn to_model_units(model: &CoordinateModel, world: &AxisMap) -> Result<Vec<f64>, CoordinateError> {
    let mut replacements = world.replacements.clone();
    if let CoordinateModel::Direction(direction) = model {
        for (axis, value) in replacements.iter_mut().enumerate() {
            if world.indices.get(axis).is_some_and(|&axis| axis < 0) {
                *value = convert_units(*value, &direction.world_units()[axis], "rad")
                    .filter(|value| value.is_finite())
                    .ok_or_else(|| {
                        CoordinateError::InvalidRecord(format!(
                            "invalid direction world replacement for local axis {axis}"
                        ))
                    })?;
            }
        }
    }
    Ok(replacements)
}

fn to_record_units(model: &CoordinateModel, values: Vec<f64>) -> Vec<f64> {
    if let (CoordinateModel::Direction(direction), [longitude, latitude]) =
        (model, values.as_slice())
    {
        return direction.in_world_units([*longitude, *latitude]).to_vec();
    }
    values
}

fn convert_units(value: f64, from: &str, to: &str) -> Option<f64> {
    if from == to {
        return Some(value);
    }
    let value = Quantity::new(value, from).ok()?;
    let to = Unit::new(to).ok()?;
    if !value.unit().conformant(&to) {
        return None;
    }
    value.get_value_in(&to).ok()
}

#[derive(Debug, Clone)]
pub(super) struct Layout {
    pub(super) placements: Vec<Placement>,
    pub(super) n_pixel: usize,
    pub(super) n_world: usize,
    pub(super) contiguous: bool,
}

impl Layout {
    pub(super) fn from_placements(placements: Vec<Placement>) -> Result<Self, CoordinateError> {
        let pixel = || {
            placements
                .iter()
                .flat_map(|map| map.pixel.indices.iter().copied())
        };
        let world = || {
            placements
                .iter()
                .flat_map(|map| map.world.indices.iter().copied())
        };
        let n_pixel = validate_numbering(pixel(), "pixel")?;
        let n_world = validate_numbering(world(), "world")?;
        let contiguous = pixel().eq(0..n_pixel as i32) && world().eq(0..n_world as i32);
        Ok(Self {
            placements,
            n_pixel,
            n_world,
            contiguous,
        })
    }
}

impl super::CoordinateSystem {
    /// For each of this coordinate's local pixel axes, return its public
    /// pixel axis, or `None` if that axis was removed.
    pub fn pixel_axes(&self, coordinate: usize) -> Vec<Option<usize>> {
        self.public_axes(coordinate, false)
    }

    /// For each of this coordinate's local world axes, return its public
    /// world axis, or `None` if that axis was removed.
    pub fn world_axes(&self, coordinate: usize) -> Vec<Option<usize>> {
        self.public_axes(coordinate, true)
    }

    fn public_axes(&self, coordinate: usize, world: bool) -> Vec<Option<usize>> {
        if let Some(layout) = &self.layout {
            let placement = &layout.placements[coordinate];
            let map = if world {
                &placement.world
            } else {
                &placement.pixel
            };
            return visible_indices(&map.indices);
        }
        let model = &self.coordinates[coordinate];
        let count = |model: &CoordinateModel| {
            if world {
                model.n_world_axes()
            } else {
                model.n_pixel_axes()
            }
        };
        let first = self.coordinates[..coordinate]
            .iter()
            .map(count)
            .sum::<usize>();
        (first..first + count(model)).map(Some).collect()
    }

    /// Returns the owning coordinate and local axis of a public pixel axis.
    pub fn pixel_axis(&self, axis: usize) -> Option<(usize, usize)> {
        self.owning_axis(axis, false)
    }

    /// Returns the owning coordinate and local axis of a public world axis.
    pub fn world_axis(&self, axis: usize) -> Option<(usize, usize)> {
        self.owning_axis(axis, true)
    }

    pub(super) fn owning_axis(&self, axis: usize, world: bool) -> Option<(usize, usize)> {
        if let Some(layout) = &self.layout {
            let axis = i32::try_from(axis).ok()?;
            return layout
                .placements
                .iter()
                .enumerate()
                .find_map(|(coordinate, placement)| {
                    let map = if world {
                        &placement.world
                    } else {
                        &placement.pixel
                    };
                    map.indices
                        .iter()
                        .position(|&value| value == axis)
                        .map(|local| (coordinate, local))
                });
        }
        let mut local = axis;
        for (coordinate, model) in self.coordinates.iter().enumerate() {
            let count = if world {
                model.n_world_axes()
            } else {
                model.n_pixel_axes()
            };
            if local < count {
                return Some((coordinate, local));
            }
            local -= count;
        }
        None
    }

    /// Maps a public pixel axis to its corresponding visible world axis.
    /// Returns `None` for an invalid axis or an absent counterpart.
    pub fn pixel_axis_to_world(&self, axis: usize) -> Option<usize> {
        let (coordinate, local) = self.pixel_axis(axis)?;
        self.world_axes(coordinate).get(local).copied().flatten()
    }

    /// Maps a public world axis to its corresponding visible pixel axis.
    /// Returns `None` for an invalid axis or an absent counterpart.
    pub fn world_axis_to_pixel(&self, axis: usize) -> Option<usize> {
        let (coordinate, local) = self.world_axis(axis)?;
        self.pixel_axes(coordinate).get(local).copied().flatten()
    }
}

pub(super) fn visible_indices(indices: &[i32]) -> Vec<Option<usize>> {
    indices
        .iter()
        .map(|&axis| usize::try_from(axis).ok())
        .collect()
}

impl super::CoordinateSystem {
    /// Local reference pixels, with removed axes filled from the pixel map's
    /// replacements. This is the local input for visiting a model off-reference
    /// without projecting unrelated coordinates.
    pub fn local_reference_pixel(&self, index: usize) -> Vec<f64> {
        let mut reference = self.coordinate(index).reference_pixel();
        if let Some(layout) = &self.layout {
            let map = &layout.placements[index].pixel;
            for (axis, (&index, value)) in map.indices.iter().zip(&mut reference).enumerate() {
                if index < 0 {
                    *value = map.replacements[axis];
                }
            }
        }
        reference
    }

    /// Reference pixels in public pixel axis order.
    pub fn reference_pixel(&self) -> Vec<f64> {
        self.collect_axis_values(false, CoordinateModel::reference_pixel)
    }

    /// Reference values in public world axis order, in the same units as
    /// [`Self::to_world`].
    pub fn reference_value(&self) -> Vec<f64> {
        self.collect_axis_values(true, CoordinateModel::reference_value)
    }

    /// Coordinate increments in public world axis order, in the same units
    /// as [`Self::to_world`].
    pub fn increment(&self) -> Vec<f64> {
        self.collect_axis_values(true, CoordinateModel::increment)
    }

    /// Units of the values returned by [`Self::to_world`], in world axis order.
    pub fn world_axis_units(&self) -> Vec<String> {
        self.collect_axis_values(true, CoordinateModel::axis_units)
    }

    /// Names in public world axis order.
    pub fn world_axis_names(&self) -> Vec<String> {
        self.collect_axis_values(true, CoordinateModel::axis_names)
    }

    fn collect_axis_values<T: Default>(
        &self,
        world: bool,
        values: impl Fn(&CoordinateModel) -> Vec<T>,
    ) -> Vec<T> {
        let Some(layout) = &self.layout else {
            return self.coordinates.iter().flat_map(values).collect();
        };
        let count = if world {
            layout.n_world
        } else {
            layout.n_pixel
        };
        let mut output: Vec<_> = std::iter::repeat_with(T::default).take(count).collect();
        for (coordinate, placement) in self.coordinates.iter().zip(&layout.placements) {
            let map = if world {
                &placement.world
            } else {
                &placement.pixel
            };
            for (&axis, value) in map.indices.iter().zip(values(coordinate)) {
                if let Ok(axis) = usize::try_from(axis) {
                    output[axis] = value;
                }
            }
        }
        output
    }
}

impl super::CoordinateSystem {
    /// Permute public world and pixel axes independently. Each entry names
    /// the old public axis at that position in the new order. Local axes and
    /// replacement values retain their order.
    ///
    /// # Errors
    /// Returns an error unless both orders are complete permutations.
    pub fn transpose_axes(
        &mut self,
        world: &[usize],
        pixel: &[usize],
    ) -> Result<(), CoordinateError> {
        let world = invert_order(world, self.n_world_axes(), "world")?;
        let pixel = invert_order(pixel, self.n_pixel_axes(), "pixel")?;
        let layout = self.explicit_layout();
        for placement in &mut layout.placements {
            for (indices, order) in [
                (&mut placement.pixel.indices, &pixel),
                (&mut placement.world.indices, &world),
            ] {
                for axis in indices.iter_mut().filter(|axis| **axis >= 0) {
                    *axis = order[*axis as usize];
                }
            }
        }
        layout.contiguous = layout
            .placements
            .iter()
            .flat_map(|p| p.pixel.indices.iter().copied())
            .eq(0..layout.n_pixel as i32)
            && layout
                .placements
                .iter()
                .flat_map(|p| p.world.indices.iter().copied())
                .eq(0..layout.n_world as i32);
        Ok(())
    }

    /// Hide a pixel axis, keeping its zero-based replacement position for
    /// forward conversion. Surviving public pixel axes are renumbered.
    ///
    /// # Errors
    /// Returns an error for an invalid axis or a non-finite replacement.
    pub fn remove_pixel_axis(
        &mut self,
        axis: usize,
        replacement: f64,
    ) -> Result<(), CoordinateError> {
        let (owner, local) = self
            .pixel_axis(axis)
            .ok_or(CoordinateError::AxisOutOfRange {
                index: axis,
                naxes: self.n_pixel_axes(),
            })?;
        require_finite(replacement)?;
        let layout = self.explicit_layout();
        for placement in &mut layout.placements {
            mark_removed(&mut placement.pixel.indices, axis as i32);
        }
        let map = &mut layout.placements[owner].pixel;
        map.replacements.resize(map.indices.len(), 0.0);
        map.replacements[local] = replacement;
        layout.n_pixel -= 1;
        layout.contiguous = false;
        Ok(())
    }

    /// Hide a world axis, retaining its replacement in its declared unit.
    /// Its corresponding pixel axis, if visible, is also removed, with the
    /// pixel position obtained by converting the world replacement while
    /// holding other visible world axes at their reference values.
    ///
    /// # Errors
    /// Returns an error for an invalid axis, a non-finite replacement, or a
    /// world replacement that cannot be projected onto a corresponding pixel.
    pub fn remove_world_axis(
        &mut self,
        axis: usize,
        replacement: f64,
    ) -> Result<(), CoordinateError> {
        let (owner, local) = self
            .world_axis(axis)
            .ok_or(CoordinateError::AxisOutOfRange {
                index: axis,
                naxes: self.n_world_axes(),
            })?;
        require_finite(replacement)?;
        let value = match self.coordinate(owner) {
            CoordinateModel::Direction(direction) => convert_units(
                replacement,
                &direction.world_units()[local],
                "rad",
            )
            .ok_or_else(|| {
                CoordinateError::InvalidRecord("invalid direction world replacement unit".into())
            })?,
            _ => replacement,
        };
        require_finite(value)?;
        if let Some(pixel_axis) = self.world_axis_to_pixel(axis) {
            let mut world = self.reference_value();
            world[axis] = value;
            let pixel = self.to_pixel(&world)?;
            self.remove_pixel_axis(pixel_axis, pixel[pixel_axis])?;
        }
        let layout = self.explicit_layout();
        for placement in &mut layout.placements {
            mark_removed(&mut placement.world.indices, axis as i32);
        }
        let placement = &mut layout.placements[owner];
        let len = placement.world.indices.len();
        placement.world.replacements.resize(len, 0.0);
        placement.model_world.resize(len, 0.0);
        placement.world.replacements[local] = replacement;
        placement.model_world[local] = value;
        layout.n_world -= 1;
        layout.contiguous = false;
        Ok(())
    }

    /// Apply a direction coordinate's absolute/relative transform to the
    /// public world values, filling its hidden components from the layout.
    ///
    /// # Errors
    /// Returns an error for an invalid world vector or non-direction model.
    pub fn convert_direction_offset(
        &self,
        index: usize,
        values: &mut [f64],
        absolute: bool,
    ) -> Result<(), CoordinateError> {
        if values.len() != self.n_world_axes() {
            return Err(CoordinateError::DimensionMismatch {
                expected: self.n_world_axes(),
                got: values.len(),
            });
        }
        let CoordinateModel::Direction(direction) = self.coordinate(index) else {
            return Err(CoordinateError::ConversionFailed(
                "offset requires a direction coordinate".into(),
            ));
        };
        if let Some(layout) = &self.layout {
            let placement = &layout.placements[index];
            let mut local: Vec<_> = placement
                .world
                .gather(|axis| values[axis], &placement.model_world)
                .collect();
            direction.convert_world_offset(&mut local, absolute)?;
            placement
                .world
                .scatter(local, |axis, value| values[axis] = value);
        } else {
            let start: usize = self.coordinates[..index]
                .iter()
                .map(CoordinateModel::n_world_axes)
                .sum();
            direction.convert_world_offset(&mut values[start..start + 2], absolute)?;
        }
        Ok(())
    }

    pub(super) fn explicit_layout(&mut self) -> &mut Layout {
        self.layout.get_or_insert_with(|| {
            let (mut pixel, mut world) = (0, 0);
            let placements = self
                .coordinates
                .iter()
                .map(|model| {
                    let placement = Placement::nominal(model, pixel, world);
                    pixel += model.n_pixel_axes();
                    world += model.n_world_axes();
                    placement
                })
                .collect();
            Box::new(Layout {
                placements,
                n_pixel: pixel,
                n_world: world,
                contiguous: true,
            })
        })
    }
}

fn invert_order(order: &[usize], rank: usize, kind: &str) -> Result<Vec<i32>, CoordinateError> {
    let invalid = || {
        CoordinateError::InvalidRecord(format!(
            "invalid {kind} axis order {order:?} for {rank} axes"
        ))
    };
    if order.len() != rank {
        return Err(invalid());
    }
    let mut inverse = vec![-1; rank];
    for (destination, &source) in order.iter().enumerate() {
        let slot = inverse.get_mut(source).ok_or_else(invalid)?;
        if *slot >= 0 {
            return Err(invalid());
        }
        *slot = destination as i32;
    }
    Ok(inverse)
}

fn mark_removed(indices: &mut [i32], removed: i32) {
    for axis in indices {
        if *axis == removed {
            *axis = -(removed + 1);
        } else if *axis > removed {
            *axis -= 1;
        }
    }
}

fn require_finite(replacement: f64) -> Result<(), CoordinateError> {
    if replacement.is_finite() {
        Ok(())
    } else {
        Err(CoordinateError::InvalidRecord(
            "axis replacement must be finite".into(),
        ))
    }
}
