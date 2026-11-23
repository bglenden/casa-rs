// SPDX-License-Identifier: LGPL-3.0-or-later

//! Casacore axis placement over full local coordinate models.

use casa_types::quanta::{Quantity, Unit};
use casa_types::{ArrayValue, RecordValue, Value};
use ndarray::{Array2, ArrayView2, ArrayViewMut2, Axis, ShapeBuilder};

use super::record_axes::{check_numbering, live_coordinate, read_models};
use super::{CoordinateSystem, RecordCoordinate, coordinate_record_entries};

mod mixed;
use crate::coordinate::CoordinateModel;
use crate::direction::convert_value_to_unit;
use crate::error::CoordinateError;
use crate::obs_info::ObsInfo;
use crate::record_utils::{get_required_vec_f64, get_required_vec_i32};

#[derive(Debug, Clone)]
struct AxisMap {
    indices: Vec<i32>,
    replacements: Vec<f64>,
}

impl AxisMap {
    fn read(
        record: &RecordValue,
        record_index: usize,
        kind: &str,
        count: usize,
        default_replacements: impl FnOnce() -> Vec<f64>,
    ) -> Result<Self, CoordinateError> {
        let map_key = format!("{kind}map{record_index}");
        let replace_key = format!("{kind}replace{record_index}");
        for key in [&map_key, &replace_key] {
            if matches!(record.get(key), Some(Value::Array(array)) if array.shape().len() != 1) {
                return Err(CoordinateError::InvalidRecord(format!(
                    "{key} must be a vector"
                )));
            }
        }
        let indices = get_required_vec_i32(record, &map_key)?;
        if indices.len() != count {
            return Err(CoordinateError::InvalidRecord(format!(
                "{map_key} names {} axes, expected {count}",
                indices.len()
            )));
        }
        let replacements = if record.get(&replace_key).is_some() {
            get_required_vec_f64(record, &replace_key)?
        } else if indices.iter().any(|&axis| axis < 0) {
            return Err(CoordinateError::InvalidRecord(format!(
                "{replace_key} is required for removed axes"
            )));
        } else {
            default_replacements()
        };
        if replacements.len() != count {
            return Err(CoordinateError::InvalidRecord(format!(
                "{replace_key} has {} entries, expected {count}",
                replacements.len()
            )));
        }
        if indices
            .iter()
            .zip(&replacements)
            .any(|(&axis, &value)| axis < 0 && !value.is_finite())
        {
            return Err(CoordinateError::InvalidRecord(format!(
                "{replace_key} must be finite for removed axes"
            )));
        }
        Ok(Self {
            indices,
            replacements,
        })
    }

    fn identity(start: usize, replacements: Vec<f64>) -> Self {
        let end = start + replacements.len();
        Self {
            indices: (start as i32..end as i32).collect(),
            replacements,
        }
    }

    fn write(&self, record: &mut RecordValue, record_index: usize, kind: &str) {
        record.upsert(
            format!("{kind}map{record_index}"),
            Value::Array(ArrayValue::from_i32_vec(self.indices.clone())),
        );
        record.upsert(
            format!("{kind}replace{record_index}"),
            Value::Array(ArrayValue::from_f64_vec(self.replacements.clone())),
        );
    }

    fn public_axes(&self) -> Vec<Option<usize>> {
        self.indices
            .iter()
            .map(|&index| usize::try_from(index).ok())
            .collect()
    }

    fn gather(&self, public: &[f64], replacements: &[f64]) -> Vec<f64> {
        self.indices
            .iter()
            .zip(replacements)
            .map(|(&axis, &hidden)| usize::try_from(axis).map_or(hidden, |axis| public[axis]))
            .collect()
    }

    fn scatter(&self, local: &[f64], public: &mut [f64]) {
        for (&axis, &value) in self.indices.iter().zip(local) {
            if let Ok(axis) = usize::try_from(axis) {
                public[axis] = value;
            }
        }
    }

    fn scatter_undefined(&self, public: &mut [f64]) {
        for &axis in &self.indices {
            if let Ok(axis) = usize::try_from(axis) {
                public[axis] = f64::NAN;
            }
        }
    }

    fn insert_hidden_axis(&mut self, local: usize, replacement: f64) {
        let visible = self.indices[0];
        let position = if visible < 0 { -1 - visible } else { visible };
        let marker = -position.saturating_add(local as i32) - 1;
        self.indices.insert(local, marker);
        self.replacements.insert(local, replacement);
    }
}

#[derive(Debug, Clone)]
struct Placement {
    pixel: AxisMap,
    world: AxisMap,
}

impl Placement {
    fn promote_direction_slice(&mut self, model: &mut CoordinateModel) -> bool {
        let CoordinateModel::Direction(direction) = model else {
            return false;
        };
        let Some(removed) = direction.take_removed_axis() else {
            return false;
        };
        self.pixel.insert_hidden_axis(removed.index, removed.pixel);
        let mut world = [0.0; 2];
        world[removed.index] = removed.world_rad;
        let replacement = direction.in_world_units(world)[removed.index];
        self.world.insert_hidden_axis(removed.index, replacement);
        true
    }

    fn coordinate_world_replacements(
        &self,
        model: &CoordinateModel,
    ) -> Result<Vec<f64>, CoordinateError> {
        let mut values = self.world.replacements.clone();
        if let CoordinateModel::Direction(direction) = model {
            for (local, (&axis, value)) in self.world.indices.iter().zip(&mut values).enumerate() {
                if axis < 0 {
                    *value = convert_value_to_unit(
                        *value,
                        &direction.world_units()[local],
                        "rad",
                        "worldreplace",
                    )?;
                }
            }
        } else if matches!(model, CoordinateModel::System(_))
            && self.world.indices.iter().any(|&axis| axis < 0)
        {
            let declared = model.declared_axis_units();
            let native = model.axis_units();
            for (local, (&axis, value)) in self.world.indices.iter().zip(&mut values).enumerate() {
                if axis < 0 && declared[local] != native[local] {
                    *value = convert_value_to_unit(
                        *value,
                        &declared[local],
                        &native[local],
                        "worldreplace",
                    )?;
                }
            }
        }
        Ok(values)
    }

    fn pixel_columns(
        &self,
        model: &CoordinateModel,
        input: ArrayView2<'_, f64>,
        undefined: bool,
    ) -> Result<Array2<f64>, CoordinateError> {
        let hidden = self.coordinate_world_replacements(model)?;
        let mut worlds = Array2::zeros((model.n_world_axes(), input.ncols()).f());
        for (local, (&axis, &replacement)) in self.world.indices.iter().zip(&hidden).enumerate() {
            let mut row = worlds.row_mut(local);
            if let Ok(axis) = usize::try_from(axis) {
                row.assign(&input.row(axis));
            } else {
                row.fill(replacement);
            }
        }
        let mut pixels = Array2::zeros((model.n_pixel_axes(), input.ncols()).f());
        project_coordinate_columns(model, worlds.view(), pixels.view_mut(), undefined)?;
        Ok(pixels)
    }
}

pub(super) fn project_coordinate_columns(
    model: &CoordinateModel,
    world: ArrayView2<'_, f64>,
    mut pixel: ArrayViewMut2<'_, f64>,
    undefined: bool,
) -> Result<(), CoordinateError> {
    let projected = match model {
        CoordinateModel::Linear(linear) => linear.to_pixel_columns(world, pixel.view_mut()),
        CoordinateModel::System(system) if world.ncols() != 0 => system
            .project_pixel_columns(world, undefined)
            .map(|values| pixel.assign(&values)),
        _ => {
            let mut input = Vec::new();
            for (column, mut output) in world.axis_iter(Axis(1)).zip(pixel.axis_iter_mut(Axis(1))) {
                let values = match column.as_slice() {
                    Some(values) => values,
                    None => {
                        input.clear();
                        input.extend(column.iter().copied());
                        &input
                    }
                };
                match model.to_pixel(values) {
                    Ok(values) => output
                        .iter_mut()
                        .zip(values)
                        .for_each(|(output, value)| *output = value),
                    Err(CoordinateError::SingularTransform(_)) if undefined => {
                        output.fill(f64::NAN)
                    }
                    Err(error) => return Err(error),
                }
            }
            return Ok(());
        }
    };
    match projected {
        Err(CoordinateError::SingularTransform(_)) if undefined => {
            pixel.fill(f64::NAN);
            Ok(())
        }
        result => result,
    }
}

pub(super) fn default_world_replacements(coordinate: &CoordinateModel) -> Vec<f64> {
    let world = coordinate
        .to_world(&vec![0.0; coordinate.n_pixel_axes()])
        .unwrap_or_else(|_| vec![0.0; coordinate.n_world_axes()]);
    if matches!(
        coordinate,
        CoordinateModel::Direction(_) | CoordinateModel::System(_)
    ) {
        return world
            .into_iter()
            .zip(
                coordinate
                    .axis_units()
                    .into_iter()
                    .zip(coordinate.declared_axis_units()),
            )
            .map(|(value, (native, declared))| {
                if native == declared {
                    value
                } else {
                    convert_value_to_unit(value, &native, &declared, "worldreplace")
                        .expect("direction units are angles")
                }
            })
            .collect();
    }
    world
}

#[derive(Debug, Clone)]
pub(super) struct AxisMaps {
    placements: Vec<Placement>,
    pub(super) n_pixel: usize,
    pub(super) n_world: usize,
    concatenated: bool,
}

impl AxisMaps {
    fn from_placements(placements: Vec<Placement>) -> Result<Self, CoordinateError> {
        let pixels = || {
            placements
                .iter()
                .map(|mapping| mapping.pixel.indices.as_slice())
        };
        let worlds = || {
            placements
                .iter()
                .map(|mapping| mapping.world.indices.as_slice())
        };
        check_numbering(pixels(), "pixel")?;
        check_numbering(worlds(), "world")?;
        let n_pixel = pixels().flatten().filter(|&&axis| axis >= 0).count();
        let n_world = worlds().flatten().filter(|&&axis| axis >= 0).count();
        let concatenated = is_concatenated(pixels()) && is_concatenated(worlds());
        Ok(Self {
            placements,
            n_pixel,
            n_world,
            concatenated,
        })
    }

    pub(super) fn write_record(&self, models: &[CoordinateModel], record: &mut RecordValue) {
        for (index, (model, placement)) in models.iter().zip(&self.placements).enumerate() {
            let prefix = model.coordinate_type().record_prefix();
            record.upsert(format!("{prefix}{index}"), Value::Record(model.to_record()));
            placement.pixel.write(record, index, "pixel");
            placement.world.write(record, index, "world");
        }
    }

    pub(super) fn append(&mut self, coordinate: &mut CoordinateModel) {
        let pixel = AxisMap::identity(self.n_pixel, vec![0.0; coordinate.n_pixel_axes()]);
        let world = AxisMap::identity(self.n_world, default_world_replacements(coordinate));
        self.n_pixel += coordinate.n_pixel_axes();
        self.n_world += coordinate.n_world_axes();
        let mut placement = Placement { pixel, world };
        if placement.promote_direction_slice(coordinate) {
            self.concatenated = false;
        }
        self.placements.push(placement);
    }

    pub(super) fn replace_coordinate(
        &mut self,
        index: usize,
        old: &CoordinateModel,
        new: &mut CoordinateModel,
    ) {
        let placement = &mut self.placements[index];
        replace_world_values(&mut placement.world, old, new);
        if placement.promote_direction_slice(new) {
            self.concatenated = false;
        }
    }

    fn update_order(&mut self) {
        self.concatenated =
            is_concatenated(self.placements.iter().map(|p| p.pixel.indices.as_slice()))
                && is_concatenated(self.placements.iter().map(|p| p.world.indices.as_slice()));
    }

    fn remove_axis(&mut self, axis: usize, replacement: f64, pixels: bool) {
        let axis = axis as i32;
        for placement in &mut self.placements {
            let side = if pixels {
                &mut placement.pixel
            } else {
                &mut placement.world
            };
            for (index, value) in side.indices.iter_mut().zip(&mut side.replacements) {
                if *index == axis {
                    *index = -axis - 1;
                    *value = replacement;
                } else if *index > axis {
                    *index -= 1;
                }
            }
        }
        if pixels {
            self.n_pixel -= 1;
        } else {
            self.n_world -= 1;
        }
        self.concatenated = false;
    }

    pub(super) fn to_world(
        &self,
        models: &[CoordinateModel],
        pixel: &[f64],
    ) -> Result<Vec<f64>, CoordinateError> {
        let mut world = vec![0.0; self.n_world];
        for (model, placement) in models.iter().zip(&self.placements) {
            let local = placement.pixel.gather(pixel, &placement.pixel.replacements);
            placement
                .world
                .scatter(&model.to_world(&local)?, &mut world);
        }
        Ok(world)
    }

    pub(super) fn to_pixel(
        &self,
        models: &[CoordinateModel],
        world: &[f64],
        undefined: bool,
    ) -> Result<Vec<f64>, CoordinateError> {
        let mut pixel = vec![0.0; self.n_pixel];
        for (model, placement) in models.iter().zip(&self.placements) {
            let hidden = placement.coordinate_world_replacements(model)?;
            let local = placement.world.gather(world, &hidden);
            let projected = match model {
                CoordinateModel::System(system) => system.project_pixels(&local, undefined),
                _ => model.to_pixel(&local),
            };
            match projected {
                Ok(values) => placement.pixel.scatter(&values, &mut pixel),
                Err(CoordinateError::SingularTransform(_)) if undefined => {
                    placement.pixel.scatter_undefined(&mut pixel);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(pixel)
    }

    pub(super) fn to_pixel_columns(
        &self,
        models: &[CoordinateModel],
        world: ArrayView2<'_, f64>,
        undefined: bool,
    ) -> Result<Array2<f64>, CoordinateError> {
        let mut pixels = Array2::zeros((self.n_pixel, world.ncols()).f());
        for (model, placement) in models.iter().zip(&self.placements) {
            let local_pixels = placement.pixel_columns(model, world, undefined)?;
            for (local, &axis) in placement.pixel.indices.iter().enumerate() {
                if let Ok(axis) = usize::try_from(axis) {
                    pixels.row_mut(axis).assign(&local_pixels.row(local));
                }
            }
        }
        Ok(pixels)
    }
}

fn replace_world_values(world: &mut AxisMap, old: &CoordinateModel, new: &CoordinateModel) {
    if old.coordinate_type() == new.coordinate_type() {
        let old_units = record_units(old);
        let new_units = record_units(new);
        let scaled = old_units
            .iter()
            .zip(&new_units)
            .zip(&world.replacements)
            .map(|((from, to), &value)| {
                if from == to {
                    return Some(value);
                }
                let quantity = Quantity::new(value, from).ok()?;
                let unit = Unit::new(to).ok()?;
                quantity.get_value_in(&unit).ok()
            })
            .collect::<Option<Vec<_>>>();
        if let Some(scaled) = scaled
            && world
                .indices
                .iter()
                .zip(&scaled)
                .all(|(&axis, value)| axis >= 0 || value.is_finite())
        {
            world.replacements = scaled;
            return;
        }
    }
    let reference = new.reference_and_increment_in_declared_units().0;
    for ((&axis, value), reference) in world
        .indices
        .iter()
        .zip(&mut world.replacements)
        .zip(reference)
    {
        // Keep an invalid reference from making the replacement vector unreadable.
        *value = if axis < 0 && reference.is_finite() {
            reference
        } else {
            0.0
        };
    }
}

fn is_concatenated<'a>(axes: impl Iterator<Item = &'a [i32]>) -> bool {
    axes.flatten()
        .copied()
        .enumerate()
        .all(|(index, axis)| i32::try_from(index).ok() == Some(axis))
}

fn record_units(model: &CoordinateModel) -> Vec<String> {
    if let CoordinateModel::Direction(direction) = model {
        direction.world_units().to_vec()
    } else {
        model.declared_axis_units()
    }
}

fn inverse_order(order: &[usize], rank: usize, kind: &str) -> Result<Vec<i32>, CoordinateError> {
    let invalid = || {
        CoordinateError::InvalidRecord(format!(
            "{kind} transpose order must contain each of its {rank} axes once"
        ))
    };
    if order.len() != rank {
        return Err(invalid());
    }
    let mut inverse = vec![-1; rank];
    for (new, &old) in order.iter().enumerate() {
        let axis = inverse.get_mut(old).ok_or_else(invalid)?;
        if *axis >= 0 {
            return Err(invalid());
        }
        *axis = i32::try_from(new).map_err(|_| invalid())?;
    }
    Ok(inverse)
}

fn check_removal(axis: usize, naxes: usize, replacement: f64) -> Result<(), CoordinateError> {
    if axis >= naxes {
        return Err(CoordinateError::AxisOutOfRange { index: axis, naxes });
    }
    if !replacement.is_finite() {
        return Err(CoordinateError::InvalidRecord(
            "axis replacement must be finite".into(),
        ));
    }
    Ok(())
}

fn convert_local_world_offsets(
    model: &CoordinateModel,
    values: &mut [f64],
    absolute: bool,
) -> Result<(), CoordinateError> {
    match model {
        CoordinateModel::Stokes(_) | CoordinateModel::Quality(_) => Ok(()),
        CoordinateModel::Direction(direction) => direction.convert_world_offset(values, absolute),
        CoordinateModel::System(system) => system.convert_world_offsets(values, absolute),
        _ => {
            for (value, reference) in values.iter_mut().zip(model.reference_value()) {
                if absolute {
                    *value += reference;
                } else {
                    *value -= reference;
                }
            }
            Ok(())
        }
    }
}

impl CoordinateSystem {
    pub(super) fn nonconsecutive_maps(&self) -> Option<&AxisMaps> {
        self.maps.as_deref().filter(|maps| !maps.concatenated)
    }

    /// The reference pixel in public pixel-axis order.
    pub fn reference_pixel(&self) -> Vec<f64> {
        self.reference_axes(true)
    }

    /// The reference value in public world-axis order, using each model's
    /// units (radians for directions).
    pub fn reference_value(&self) -> Vec<f64> {
        self.reference_axes(false)
    }

    /// A coordinate's local reference pixels. Removed axes use their
    /// stored pixel replacements.
    pub fn local_reference_pixel(&self, index: usize) -> Vec<f64> {
        let mut values = self.coordinate(index).reference_pixel();
        if let Some(maps) = self.nonconsecutive_maps() {
            let pixel = &maps.placements[index].pixel;
            for ((value, &axis), &replacement) in values
                .iter_mut()
                .zip(&pixel.indices)
                .zip(&pixel.replacements)
            {
                if axis < 0 {
                    *value = replacement;
                }
            }
        }
        values
    }

    /// A coordinate's stored replacement pixel vector, including entries
    /// on visible axes. Unmapped systems have no replacement vector.
    pub fn local_pixel_replacements(&self, index: usize) -> Option<&[f64]> {
        self.nonconsecutive_maps()
            .and_then(|maps| maps.placements.get(index))
            .map(|placement| placement.pixel.replacements.as_slice())
    }

    /// Units of the values returned by `to_world`, in public world-axis order.
    pub fn world_axis_units(&self) -> Vec<String> {
        self.world_axis_values(CoordinateModel::axis_units)
    }

    pub(crate) fn world_axis_values<T: Default + Clone>(
        &self,
        values: impl Fn(&CoordinateModel) -> Vec<T>,
    ) -> Vec<T> {
        let mut result = vec![T::default(); self.n_world_axes()];
        for (index, model) in self.coordinates.iter().enumerate() {
            let world = self
                .coordinate_world_axes(index)
                .expect("existing coordinate");
            for (axis, value) in world.into_iter().zip(values(model)) {
                if let Some(axis) = axis {
                    result[axis] = value;
                }
            }
        }
        result
    }

    fn reference_axes(&self, pixel: bool) -> Vec<f64> {
        let local = if pixel {
            CoordinateModel::reference_pixel
        } else {
            CoordinateModel::reference_value
        };
        let Some(maps) = self.nonconsecutive_maps() else {
            return self.coordinates.iter().flat_map(local).collect();
        };
        let mut result = vec![0.0; if pixel { maps.n_pixel } else { maps.n_world }];
        for (model, placement) in self.coordinates.iter().zip(&maps.placements) {
            let axes = if pixel {
                &placement.pixel
            } else {
                &placement.world
            };
            axes.scatter(&local(model), &mut result);
        }
        result
    }

    /// Convert between absolute worlds and offsets from the reference
    /// coordinate, using each model's units (radians for directions).
    ///
    /// # Errors
    /// Returns an error if the vector has the wrong rank or a direction
    /// cannot be converted.
    pub fn convert_world_offsets(
        &self,
        world: &mut [f64],
        absolute: bool,
    ) -> Result<(), CoordinateError> {
        if world.len() != self.n_world_axes() {
            return Err(CoordinateError::DimensionMismatch {
                expected: self.n_world_axes(),
                got: world.len(),
            });
        }
        if let Some(maps) = self.nonconsecutive_maps() {
            for (model, placement) in self.coordinates.iter().zip(&maps.placements) {
                let hidden = placement.coordinate_world_replacements(model)?;
                let mut values = placement.world.gather(world, &hidden);
                convert_local_world_offsets(model, &mut values, absolute)?;
                placement.world.scatter(&values, world);
            }
        } else {
            let mut rest = world;
            for model in &self.coordinates {
                let (values, next) = rest.split_at_mut(model.n_world_axes());
                convert_local_world_offsets(model, values, absolute)?;
                rest = next;
            }
        }
        Ok(())
    }

    /// The fields and maps of this system's record, in this system's
    /// coordinate order. Pass the record used to build the system.
    ///
    /// # Errors
    /// The record is malformed or has a different number of coordinates.
    pub fn coordinates_in_record(
        &self,
        record: &RecordValue,
    ) -> Result<Vec<RecordCoordinate>, CoordinateError> {
        let Some(maps) = &self.maps else {
            return Self::record_coordinates(record);
        };
        let fields = coordinate_record_entries(record);
        if fields.len() != maps.placements.len() {
            return Err(CoordinateError::InvalidRecord(
                "coordinate record does not match the system's coordinate count".into(),
            ));
        }
        Ok(fields
            .into_iter()
            .zip(&maps.placements)
            .map(|((_, field), placed)| RecordCoordinate {
                field,
                pixel_axes: placed.pixel.public_axes(),
                world_axes: placed.world.public_axes(),
            })
            .collect())
    }

    /// Restore an edited record using the same axis layout as this system.
    ///
    /// # Errors
    /// The edited record or its coordinates are invalid.
    pub fn restore_record_with_layout(
        &self,
        record: &RecordValue,
    ) -> Result<Self, CoordinateError> {
        if self.maps.is_some() {
            Self::from_mapped_record(record)
        } else {
            Self::restore_record(record, false)
        }
    }

    /// The public world axis corresponding to a public pixel axis.
    pub fn pixel_axis_to_world_axis(&self, axis: usize) -> Option<usize> {
        let (index, local) = self.pixel_axis_owner(axis)?;
        self.coordinate_world_axes(index)?
            .get(local)
            .copied()
            .flatten()
    }

    /// The public pixel axis corresponding to a public world axis.
    pub fn world_axis_to_pixel_axis(&self, axis: usize) -> Option<usize> {
        let (index, local) = self.world_axis_owner(axis)?;
        self.coordinate_pixel_axes(index)?
            .get(local)
            .copied()
            .flatten()
    }

    fn ensure_axis_maps(&mut self) -> &mut AxisMaps {
        if self.maps.is_none() {
            let mut maps = AxisMaps::from_placements(Vec::new()).expect("empty maps are valid");
            for coordinate in &mut self.coordinates {
                maps.append(coordinate);
            }
            self.maps = Some(Box::new(maps));
        }
        self.maps.as_deref_mut().expect("axis maps initialized")
    }

    /// Reads a casacore record in coordinate creation order, retaining
    /// separate pixel and world maps and each displaced axis's replacement.
    ///
    /// # Errors
    /// A model, axis permutation, replacement vector or observation record is invalid.
    pub fn from_mapped_record(record: &RecordValue) -> Result<Self, CoordinateError> {
        Self::restore_mapped_record(record, false)
    }

    pub(super) fn restore_mapped_record(
        record: &RecordValue,
        live: bool,
    ) -> Result<Self, CoordinateError> {
        let obs_info = match record.get("obsinfo") {
            Some(Value::Record(obs)) => ObsInfo::from_record(obs)?,
            _ => ObsInfo::from_record(record)?,
        };
        let models = read_models(record)?;
        let has_maps = models.iter().any(|(index, _, _)| {
            record.get(&format!("pixelmap{index}")).is_some()
                || record.get(&format!("worldmap{index}")).is_some()
        });
        let mut pixel_start = 0;
        let mut world_start = 0;
        let mut placements = Vec::with_capacity(models.len());
        let mut coordinates = Vec::with_capacity(models.len());
        for (expected, (record_index, field, mut model)) in models.into_iter().enumerate() {
            if record_index != expected {
                return Err(CoordinateError::InvalidRecord(
                    "coordinate records must be numbered from zero without gaps".into(),
                ));
            }
            if live {
                model = live_coordinate(record, &field, model)?;
            }
            let pixel_rank = model.n_pixel_axes();
            let world_rank = model.n_world_axes();
            let (pixel, world) = if has_maps {
                (
                    AxisMap::read(record, record_index, "pixel", pixel_rank, || {
                        vec![0.0; pixel_rank]
                    })?,
                    AxisMap::read(record, record_index, "world", world_rank, || {
                        default_world_replacements(&model)
                    })?,
                )
            } else {
                (
                    AxisMap::identity(pixel_start, vec![0.0; pixel_rank]),
                    AxisMap::identity(world_start, default_world_replacements(&model)),
                )
            };
            pixel_start += pixel_rank;
            world_start += world_rank;
            placements.push(Placement { pixel, world });
            coordinates.push(model);
        }
        let maps = AxisMaps::from_placements(placements)?;
        Ok(Self {
            coordinates,
            obs_info,
            maps: Some(Box::new(maps)),
        })
    }

    /// Converts a matrix of pixel coordinates, one point per column.
    /// The output rows follow the system's world axes, with columns contiguous.
    ///
    /// # Errors
    /// The input has the wrong number of pixel rows, the output cannot be
    /// allocated, or a point cannot be converted.
    pub fn to_world_columns(
        &self,
        pixel: ArrayView2<'_, f64>,
    ) -> Result<Array2<f64>, CoordinateError> {
        let n_pixel = self.n_pixel_axes();
        if pixel.nrows() != n_pixel {
            return Err(CoordinateError::DimensionMismatch {
                expected: n_pixel,
                got: pixel.nrows(),
            });
        }
        let n_world = self.n_world_axes();
        let size = n_world.checked_mul(pixel.ncols()).ok_or_else(|| {
            CoordinateError::ConversionFailed("world coordinate matrix size overflow".into())
        })?;
        let mut result = Vec::new();
        result
            .try_reserve_exact(size)
            .map_err(|error| CoordinateError::ConversionFailed(error.to_string()))?;
        let mut buffer = Vec::new();
        let maps = self.nonconsecutive_maps();
        for column in pixel.axis_iter(Axis(1)) {
            let column = if let Some(data) = column.as_slice() {
                data
            } else {
                buffer.clear();
                buffer.extend(column.iter().copied());
                &buffer
            };
            if let Some(maps) = maps {
                result.extend(maps.to_world(&self.coordinates, column)?);
            } else {
                self.append_consecutive_world(column, &mut result)?;
            }
        }
        Array2::from_shape_vec((n_world, pixel.ncols()).f(), result)
            .map_err(|error| CoordinateError::ConversionFailed(error.to_string()))
    }

    /// Reorders the world and pixel axes independently. Each entry names
    /// the old public axis to place at that position in the new order.
    ///
    /// # Errors
    /// An order is not a permutation of the corresponding axes.
    pub fn transpose_axes(
        &mut self,
        world_order: &[usize],
        pixel_order: &[usize],
    ) -> Result<(), CoordinateError> {
        let world = inverse_order(world_order, self.n_world_axes(), "world")?;
        let pixel = inverse_order(pixel_order, self.n_pixel_axes(), "pixel")?;
        let maps = self.ensure_axis_maps();
        for placement in &mut maps.placements {
            for (axis_map, order) in [
                (&mut placement.world, &world),
                (&mut placement.pixel, &pixel),
            ] {
                for axis in &mut axis_map.indices {
                    if let Ok(old) = usize::try_from(*axis) {
                        *axis = order[old];
                    }
                }
            }
        }
        maps.update_order();
        Ok(())
    }

    /// Removes a public pixel axis, using `replacement` for its local pixel
    /// during forward conversion. The world axes are unaffected.
    ///
    /// # Errors
    /// The axis is out of range or the replacement is non-finite.
    pub fn remove_pixel_axis(
        &mut self,
        axis: usize,
        replacement: f64,
    ) -> Result<(), CoordinateError> {
        check_removal(axis, self.n_pixel_axes(), replacement)?;
        self.ensure_axis_maps().remove_axis(axis, replacement, true);
        Ok(())
    }

    /// Removes a public world axis. If its corresponding pixel axis is
    /// visible, it is removed at the pixel corresponding to this world
    /// value and the other axes' reference values. Direction replacements
    /// are passed in radians, as in `to_pixel`.
    ///
    /// # Errors
    /// The axis or replacement is invalid, or projecting the replacement fails.
    pub fn remove_world_axis(
        &mut self,
        axis: usize,
        replacement: f64,
    ) -> Result<(), CoordinateError> {
        check_removal(axis, self.n_world_axes(), replacement)?;
        let (index, local) = self.world_axis_owner(axis).expect("validated world axis");
        let pixel_axis = self
            .coordinate_pixel_axes(index)
            .and_then(|axes| axes.get(local).copied().flatten());
        let pixel = pixel_axis
            .map(|pixel_axis| {
                let mut world = vec![0.0; self.n_world_axes()];
                for (index, coordinate) in self.coordinates.iter().enumerate() {
                    let axes = self
                        .coordinate_world_axes(index)
                        .expect("existing coordinate");
                    for (axis, value) in axes.into_iter().zip(coordinate.reference_value()) {
                        if let Some(axis) = axis {
                            world[axis] = value;
                        }
                    }
                }
                world[axis] = replacement;
                let position = self.to_pixel(&world)?[pixel_axis];
                check_removal(pixel_axis, self.n_pixel_axes(), position)?;
                Ok::<_, CoordinateError>((pixel_axis, position))
            })
            .transpose()?;

        let model = &self.coordinates[index];
        let replacement = if matches!(
            model,
            CoordinateModel::Direction(_) | CoordinateModel::System(_)
        ) {
            let native = model.axis_units();
            let declared = model.declared_axis_units();
            if native[local] == declared[local] {
                replacement
            } else {
                convert_value_to_unit(
                    replacement,
                    &native[local],
                    &declared[local],
                    "worldreplace",
                )?
            }
        } else {
            replacement
        };
        check_removal(axis, self.n_world_axes(), replacement)?;
        let maps = self.ensure_axis_maps();
        if let Some((axis, position)) = pixel {
            maps.remove_axis(axis, position, true);
        }
        maps.remove_axis(axis, replacement, false);
        Ok(())
    }

    /// The public pixel axis for each local axis of a coordinate.
    /// A removed axis is represented by `None`.
    pub fn coordinate_pixel_axes(&self, index: usize) -> Option<Vec<Option<usize>>> {
        self.coordinate_axes(index, true)
    }

    /// The public world axis for each local axis of a coordinate.
    /// A removed axis is represented by `None`.
    pub fn coordinate_world_axes(&self, index: usize) -> Option<Vec<Option<usize>>> {
        self.coordinate_axes(index, false)
    }

    /// Finds the coordinate and local axis that own a public pixel axis.
    pub fn pixel_axis_owner(&self, axis: usize) -> Option<(usize, usize)> {
        self.axis_owner(axis, true)
    }

    /// Finds the coordinate and local axis that own a public world axis.
    pub fn world_axis_owner(&self, axis: usize) -> Option<(usize, usize)> {
        self.axis_owner(axis, false)
    }

    fn axis_owner(&self, axis: usize, pixels: bool) -> Option<(usize, usize)> {
        if let Some(maps) = &self.maps {
            let axis = i32::try_from(axis).ok()?;
            return maps
                .placements
                .iter()
                .enumerate()
                .find_map(|(index, placement)| {
                    let side = if pixels {
                        &placement.pixel
                    } else {
                        &placement.world
                    };
                    side.indices
                        .iter()
                        .position(|&value| value == axis)
                        .map(|local| (index, local))
                });
        }
        let mut offset = 0;
        for (index, coordinate) in self.coordinates.iter().enumerate() {
            let count = if pixels {
                coordinate.n_pixel_axes()
            } else {
                coordinate.n_world_axes()
            };
            let next = offset + count;
            if (offset..next).contains(&axis) {
                return Some((index, axis - offset));
            }
            offset = next;
        }
        None
    }

    fn coordinate_axes(&self, index: usize, pixels: bool) -> Option<Vec<Option<usize>>> {
        let coordinate = self.coordinates.get(index)?;
        if let Some(maps) = &self.maps {
            let placement = &maps.placements[index];
            return Some(
                if pixels {
                    &placement.pixel
                } else {
                    &placement.world
                }
                .public_axes(),
            );
        }
        let count = |model: &CoordinateModel| {
            if pixels {
                model.n_pixel_axes()
            } else {
                model.n_world_axes()
            }
        };
        let start: usize = self.coordinates[..index].iter().map(count).sum();
        Some((start..start + count(coordinate)).map(Some).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DirectionCoordinate, LinearCoordinate, Projection, ProjectionType, TabularCoordinate,
    };
    use casa_types::measures::direction::DirectionRef;
    use ndarray::array;

    fn set_map(record: &mut RecordValue, key: &str, axes: &[i32]) {
        record.upsert(key, Value::Array(ArrayValue::from_i32_vec(axes.to_vec())));
    }

    fn set_replacements(record: &mut RecordValue, key: &str, replacements: &[f64]) {
        record.upsert(
            key,
            Value::Array(ArrayValue::from_f64_vec(replacements.to_vec())),
        );
    }

    #[test]
    fn world_columns_preserve_layout_values_and_error_order() {
        let system = crate::defaults::coordinate_system_for_shape(&[19, 13, 2, 6], false);
        let pixels = array![
            [9.0, 9.25, 7.5],
            [6.0, 5.5, 7.0],
            [0.0, 1.0, 1.0],
            [0.0, 2.5, 4.0],
        ];
        let column_major = Array2::from_shape_fn(pixels.dim().f(), |index| pixels[index]);
        for input in [pixels.view(), column_major.view()] {
            let world = system.to_world_columns(input).unwrap();
            assert!(world.view().reversed_axes().is_standard_layout());
            for (pixel, transformed) in input.axis_iter(Axis(1)).zip(world.axis_iter(Axis(1))) {
                let expected = system.to_world(&pixel.to_vec()).unwrap();
                assert_eq!(transformed.as_slice().unwrap(), expected);
            }
            assert_eq!(world.row(2).to_vec(), vec![1.0, 2.0, 2.0]);
        }
        let mut invalid = pixels;
        invalid[[2, 0]] = 12.0;
        invalid[[0, 1]] = 2e6;
        assert!(matches!(
            system.to_world_columns(invalid.view()),
            Err(CoordinateError::AxisOutOfRange { .. })
        ));
    }

    #[test]
    fn nested_inverse_columns_keep_valid_axes_beside_a_singular_axis() {
        let mut child = CoordinateSystem::new();
        child.add_coordinate(
            LinearCoordinate::new(1, vec!["Singular".into()], vec!["m".into()])
                .with_pc_matrix(array![[0.0]]),
        );
        child.add_coordinate(
            LinearCoordinate::new(1, vec!["Valid".into()], vec!["s".into()])
                .with_reference_value(vec![5.0])
                .with_increment(vec![2.0]),
        );
        let mut system = CoordinateSystem::new();
        system.add_coordinate(child);
        assert!(system.to_pixel(&[0.0, 7.0]).is_err());
        let pixels = system.to_pixel_or_nan(&[0.0, 7.0]).unwrap();
        assert!(pixels[0].is_nan());
        assert_eq!(pixels[1], 1.0);
        let original = system
            .to_pixel_columns_or_nan(array![[0.0, 0.0], [7.0, 11.0]].view())
            .unwrap();
        assert!(original.row(0).iter().all(|value| value.is_nan()));
        assert_eq!(original.row(1), array![1.0, 3.0]);
        system.transpose_axes(&[1, 0], &[1, 0]).unwrap();
        let mapped = system
            .to_pixel_columns_or_nan(array![[7.0, 11.0], [0.0, 0.0]].view())
            .unwrap();
        assert_eq!(mapped.row(0), array![1.0, 3.0]);
        assert!(mapped.row(1).iter().all(|value| value.is_nan()));
    }

    #[test]
    fn hidden_linear_axes_use_separate_pixel_and_world_replacements() {
        let mut coords = CoordinateSystem::new();
        coords.add_coordinate(
            LinearCoordinate::new(2, vec!["x".into(), "y".into()], vec!["pix".into(); 2])
                .with_reference_value(vec![100.0, 200.0])
                .with_pc_matrix(array![[2.0, 1.0], [1.0, 3.0]]),
        );
        let mut record = coords.to_record();
        set_map(&mut record, "pixelmap0", &[0, -2]);
        set_map(&mut record, "worldmap0", &[0, -2]);
        set_replacements(&mut record, "pixelreplace0", &[11.0, 4.0]);
        set_replacements(&mut record, "worldreplace0", &[123.0, 230.0]);
        let mapped = CoordinateSystem::from_mapped_record(&record).unwrap();
        assert_eq!((mapped.n_pixel_axes(), mapped.n_world_axes()), (1, 1));
        assert_eq!(mapped.coordinate_pixel_axes(0), Some(vec![Some(0), None]));
        assert_eq!(mapped.pixel_axis_owner(0), Some((0, 0)));
        assert_eq!(mapped.world_axis_owner(1), None);
        assert_eq!(mapped.to_world(&[7.0]).unwrap(), [118.0]);
        assert!((mapped.to_pixel(&[118.0]).unwrap()[0] - 4.8).abs() < 1e-12);
        assert_eq!(
            mapped.to_world_columns(array![[7.0, 8.0]].view()).unwrap(),
            array![[118.0, 120.0]]
        );
        let inverse = mapped
            .to_pixel_columns(array![[118.0, 120.0]].view())
            .unwrap();
        for (actual, expected) in inverse.iter().zip([4.8, 6.0]) {
            assert!((actual - expected).abs() < 1e-12);
        }
        let saved = mapped.to_record();
        for field in ["pixelmap0", "worldmap0", "pixelreplace0", "worldreplace0"] {
            assert_eq!(record.get(field), saved.get(field));
        }
    }

    #[test]
    fn direction_world_replacement_uses_declared_degrees() {
        let mut coords = CoordinateSystem::new();
        let longitude_reference = 10_f64.to_radians();
        let latitude_reference = 30_f64.to_radians();
        coords.add_coordinate(
            DirectionCoordinate::new(
                DirectionRef::J2000,
                Projection::new(ProjectionType::SIN),
                [longitude_reference, latitude_reference],
                [-0.01, 0.01],
                [5.0, 10.0],
            )
            .with_world_units(["deg", "deg"])
            .unwrap(),
        );
        let mut record = coords.to_record();
        set_map(&mut record, "pixelmap0", &[-1, 0]);
        set_map(&mut record, "worldmap0", &[-1, 0]);
        set_replacements(&mut record, "pixelreplace0", &[5.0, 0.0]);
        set_replacements(&mut record, "worldreplace0", &[35.0, 0.0]);
        let mapped = CoordinateSystem::from_mapped_record(&record).unwrap();
        let latitude = 38_f64.to_radians();
        let delta = 35_f64.to_radians() - longitude_reference;
        let m = latitude.sin() * latitude_reference.cos()
            - latitude.cos() * latitude_reference.sin() * delta.cos();
        let expected = 10.0 + m / 0.01;
        let actual = mapped.to_pixel(&[latitude]).unwrap()[0];
        assert!((actual - expected).abs() < 1e-10);
        assert!((mapped.to_world(&[10.0]).unwrap()[0] - latitude_reference).abs() < 1e-12);
        assert_eq!(
            mapped.to_record().get("worldreplace0"),
            record.get("worldreplace0")
        );
    }

    #[test]
    fn world_and_pixel_permutations_are_independent_and_can_have_different_ranks() {
        let mut coords = CoordinateSystem::new();
        coords.add_coordinate(LinearCoordinate::new(2, Vec::new(), Vec::new()));
        coords.add_coordinate(LinearCoordinate::new(1, Vec::new(), Vec::new()));
        let mut record = coords.to_record();
        set_map(&mut record, "pixelmap0", &[-1, 1]);
        set_map(&mut record, "pixelmap1", &[0]);
        set_map(&mut record, "worldmap0", &[2, 0]);
        set_map(&mut record, "worldmap1", &[1]);
        set_replacements(&mut record, "pixelreplace0", &[42.0, 0.0]);
        let mapped = CoordinateSystem::from_mapped_record(&record).unwrap();
        assert_eq!(mapped.to_world(&[4.0, 7.0]).unwrap(), [7.0, 4.0, 42.0]);
        assert_eq!(mapped.to_pixel(&[8.0, 4.0, 20.0]).unwrap(), [4.0, 8.0]);
        assert_eq!(mapped.world_axis_owner(2), Some((0, 0)));
        assert_eq!(mapped.pixel_axis_owner(0), Some((1, 0)));
        assert_eq!(
            mapped.coordinate_world_axes(0),
            Some(vec![Some(2), Some(0)])
        );

        record.remove("pixelreplace0");
        assert!(
            CoordinateSystem::from_mapped_record(&record)
                .unwrap_err()
                .to_string()
                .contains("pixelreplace0")
        );
        set_replacements(&mut record, "pixelreplace0", &[42.0, 0.0]);
        set_map(&mut record, "worldmap1", &[2]);
        assert!(
            CoordinateSystem::from_mapped_record(&record)
                .unwrap_err()
                .to_string()
                .contains("world maps")
        );
    }

    #[test]
    fn edits_use_public_axes_and_retain_repeated_removal_markers() {
        let mut mapped = CoordinateSystem::new();
        mapped.add_coordinate(LinearCoordinate::new(3, Vec::new(), Vec::new()));
        let original = mapped.to_record();
        assert!(mapped.transpose_axes(&[0, 0, 2], &[0, 1, 2]).is_err());
        assert_eq!(mapped.to_record(), original);
        mapped.transpose_axes(&[2, 0, 1], &[1, 2, 0]).unwrap();
        assert_eq!(
            mapped.to_world(&[10.0, 20.0, 30.0]).unwrap(),
            [20.0, 30.0, 10.0]
        );
        assert_eq!(
            mapped.to_pixel(&[20.0, 30.0, 10.0]).unwrap(),
            [10.0, 20.0, 30.0]
        );
        mapped.remove_pixel_axis(0, 40.0).unwrap();
        mapped.remove_pixel_axis(0, 50.0).unwrap();
        assert_eq!(mapped.to_world(&[60.0]).unwrap(), [50.0, 60.0, 40.0]);
        let record = mapped.to_record();
        assert_eq!(
            get_required_vec_i32(&record, "pixelmap0").unwrap(),
            [0, -1, -1]
        );
        assert_eq!(
            get_required_vec_f64(&record, "pixelreplace0").unwrap(),
            [0.0, 40.0, 50.0]
        );
        let reopened = CoordinateSystem::from_mapped_record(&record).unwrap();
        assert_eq!(reopened.to_world(&[60.0]).unwrap(), [50.0, 60.0, 40.0]);
    }

    #[test]
    fn removing_world_axis_projects_its_value_before_removing_pixel_axis() {
        let mut mapped = CoordinateSystem::new();
        mapped.add_coordinate(
            LinearCoordinate::new(2, Vec::new(), Vec::new())
                .with_reference_value(vec![100.0, 200.0])
                .with_pc_matrix(array![[2.0, 1.0], [1.0, 3.0]]),
        );
        mapped.remove_world_axis(1, 230.0).unwrap();
        let record = mapped.to_record();
        assert!((get_required_vec_f64(&record, "pixelreplace0").unwrap()[1] - 12.0).abs() < 1e-12);
        assert_eq!(get_required_vec_i32(&record, "worldmap0").unwrap(), [0, -2]);
        assert_eq!(mapped.to_world(&[4.0]).unwrap(), [120.0]);
        assert!((mapped.to_pixel(&[120.0]).unwrap()[0] - 6.0).abs() < 1e-12);
    }

    #[test]
    fn whole_coordinate_replacement_converts_or_resets_hidden_world_values() {
        let linear = |units: [&str; 2]| {
            LinearCoordinate::new(2, Vec::new(), units.map(str::to_owned).into())
                .with_reference_value(vec![10.0, 20.0])
        };
        let mut mapped = CoordinateSystem::new();
        mapped.add_coordinate(linear(["MHz", "m"]));
        mapped.remove_world_axis(0, 42.0).unwrap();
        mapped.replace_coordinate(0, linear(["kHz", "m"]));
        let raw = mapped.to_record();
        assert!((get_required_vec_f64(&raw, "worldreplace0").unwrap()[0] - 42_000.0).abs() < 1e-9);
        assert_eq!(
            get_required_vec_f64(&raw, "pixelreplace0").unwrap()[0],
            32.0
        );
        mapped.replace_coordinate(0, linear(["s", "m"]));
        assert_eq!(
            get_required_vec_f64(&mapped.to_record(), "worldreplace0").unwrap(),
            [10.0, 0.0]
        );
    }

    #[test]
    fn mapless_and_all_visible_records_need_no_replacement_fields() {
        let mut source = CoordinateSystem::new();
        source.add_coordinate(LinearCoordinate::new(2, Vec::new(), Vec::new()));
        let mut record = source.to_record();
        record.remove("pixelreplace0");
        record.remove("worldreplace0");
        let explicit = CoordinateSystem::from_mapped_record(&record).unwrap();
        assert!(explicit.nonconsecutive_maps().is_none());
        record.upsert(
            "pixelmap0",
            Value::Array(ArrayValue::Int32(array![[0, 1]].into_dyn())),
        );
        assert!(CoordinateSystem::from_mapped_record(&record).is_err());
        record.remove("pixelmap0");
        record.remove("worldmap0");
        let mapless = CoordinateSystem::from_mapped_record(&record).unwrap();
        assert_eq!(mapless.to_world(&[2.0, 3.0]).unwrap(), [2.0, 3.0]);
        assert_eq!(mapless.to_record(), explicit.to_record());
    }

    fn legacy_direction_slice() -> DirectionCoordinate {
        let mut direction = DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [0.2, 0.3],
            [-0.01, 0.01],
            [0.0, 1.0],
        )
        .with_world_units(["deg", "deg"])
        .unwrap();
        direction.remove_record_axis(0, 4.0, 13.0).unwrap();
        direction
    }

    #[test]
    fn promoting_legacy_slice_preserves_other_live_coordinate_models() {
        let mut system = CoordinateSystem::new();
        system.add_coordinate(legacy_direction_slice());
        system.add_coordinate(
            TabularCoordinate::linear(0.0, 1.0, 0.0, "offset", "pix")
                .with_linear_transform(2.0)
                .unwrap(),
        );
        let pixel = [2.0, 3.0];
        let world = system.to_world(&pixel).unwrap();
        assert_eq!(world[1], 6.0);
        let inverse = system.to_pixel(&world).unwrap();
        system.transpose_axes(&[0, 1], &[0, 1]).unwrap();
        let actual_world = system.to_world(&pixel).unwrap();
        let actual_pixel = system.to_pixel(&world).unwrap();
        assert_eq!(actual_world[1], 6.0);
        assert_eq!(actual_pixel[1], 3.0);
        assert!((actual_world[0] - world[0]).abs() < 1e-12);
        assert!((actual_pixel[0] - inverse[0]).abs() < 1e-12);
    }

    #[test]
    fn appending_or_replacing_a_legacy_slice_saves_its_full_axis_maps() {
        for append in [false, true] {
            let mut system = CoordinateSystem::new();
            system.add_coordinate(LinearCoordinate::new(1, Vec::new(), Vec::new()));
            system.transpose_axes(&[0], &[0]).unwrap();
            if append {
                system.add_coordinate(legacy_direction_slice());
            } else {
                system.replace_coordinate(0, legacy_direction_slice());
            }
            let record = system.to_record();
            let slot = usize::from(append);
            for kind in ["pixelmap", "worldmap"] {
                assert_eq!(
                    get_required_vec_i32(&record, &format!("{kind}{slot}")).unwrap(),
                    [-(slot as i32) - 1, slot as i32]
                );
            }
            let restored = CoordinateSystem::from_mapped_record(&record).unwrap();
            let pixel = vec![2.0; system.n_pixel_axes()];
            let world = system.to_world(&pixel).unwrap();
            let restored_world = restored.to_world(&pixel).unwrap();
            let original_inverse = system.to_pixel(&world).unwrap();
            let restored_inverse = restored.to_pixel(&world).unwrap();
            for (actual, expected) in restored_world.iter().zip(world) {
                assert!((actual - expected).abs() < 1e-12);
            }
            for (actual, expected) in restored_inverse.iter().zip(original_inverse) {
                assert!((actual - expected).abs() < 1e-12);
            }
        }
    }

    #[test]
    fn mapped_coordinate_numbers_are_contiguous() {
        let mut system = CoordinateSystem::new();
        for _ in 0..3 {
            system.add_coordinate(LinearCoordinate::new(1, Vec::new(), Vec::new()));
        }
        for missing in [0, 1] {
            let mut record = system.to_record();
            record.remove(&format!("linear{missing}"));
            let error = CoordinateSystem::from_mapped_record(&record).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("numbered from zero without gaps")
            );
        }
    }

    #[test]
    fn replacing_with_an_invalid_reference_keeps_the_hidden_slot_finite() {
        let mut system = CoordinateSystem::new();
        system.add_coordinate(LinearCoordinate::new(
            2,
            Vec::new(),
            vec!["MHz".into(), "m".into()],
        ));
        system.remove_world_axis(0, 42.0).unwrap();
        system.replace_coordinate(
            0,
            LinearCoordinate::new(2, Vec::new(), vec!["s".into(), "m".into()])
                .with_reference_value(vec![f64::NAN, 20.0]),
        );
        let record = system.to_record();
        assert_eq!(
            get_required_vec_f64(&record, "worldreplace0").unwrap(),
            [0.0, 0.0]
        );
        assert!(CoordinateSystem::from_mapped_record(&record).is_ok());
    }
}
