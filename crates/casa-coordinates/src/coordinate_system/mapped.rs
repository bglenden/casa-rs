// SPDX-License-Identifier: LGPL-3.0-or-later

//! Casacore axis placement over full local coordinate models.

use casa_types::{ArrayValue, RecordValue, Value};
use ndarray::{Array2, ArrayView2, Axis, ShapeBuilder};

use super::record_axes::{check_numbering, read_models};
use super::{CoordinateSystem, record_basename};
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
}

#[derive(Debug, Clone)]
struct Placement {
    record_index: usize,
    pixel: AxisMap,
    world: AxisMap,
}

impl Placement {
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
        if let CoordinateModel::Linear(linear) = model {
            match linear.to_pixel_columns(worlds.view(), pixels.view_mut()) {
                Ok(()) => (),
                Err(CoordinateError::SingularTransform(_)) if undefined => pixels.fill(f64::NAN),
                Err(error) => return Err(error),
            }
        } else {
            for (world, mut pixel) in worlds.axis_iter(Axis(1)).zip(pixels.axis_iter_mut(Axis(1))) {
                match model.to_pixel(world.as_slice().expect("contiguous world column")) {
                    Ok(values) => pixel
                        .iter_mut()
                        .zip(values)
                        .for_each(|(output, value)| *output = value),
                    Err(CoordinateError::SingularTransform(_)) if undefined => pixel.fill(f64::NAN),
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(pixels)
    }
}

fn default_world_replacements(coordinate: &CoordinateModel) -> Vec<f64> {
    let world = coordinate
        .to_world(&vec![0.0; coordinate.n_pixel_axes()])
        .unwrap_or_else(|_| vec![0.0; coordinate.n_world_axes()]);
    if let CoordinateModel::Direction(direction) = coordinate
        && let [longitude, latitude] = world.as_slice()
    {
        return direction.in_world_units([*longitude, *latitude]).into();
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
        for (model, placement) in models.iter().zip(&self.placements) {
            let index = placement.record_index;
            let prefix = record_basename(model.coordinate_type());
            record.upsert(format!("{prefix}{index}"), Value::Record(model.to_record()));
            placement.pixel.write(record, index, "pixel");
            placement.world.write(record, index, "world");
        }
    }

    pub(super) fn append(&mut self, coordinate: &CoordinateModel) {
        let record_index = self.placements.last().map_or(0, |p| p.record_index + 1);
        let pixel = AxisMap::identity(self.n_pixel, vec![0.0; coordinate.n_pixel_axes()]);
        let world = AxisMap::identity(self.n_world, default_world_replacements(coordinate));
        self.placements.push(Placement {
            record_index,
            pixel,
            world,
        });
        self.n_pixel += coordinate.n_pixel_axes();
        self.n_world += coordinate.n_world_axes();
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
            match model.to_pixel(&local) {
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

fn is_concatenated<'a>(axes: impl Iterator<Item = &'a [i32]>) -> bool {
    axes.flatten()
        .copied()
        .enumerate()
        .all(|(index, axis)| i32::try_from(index).ok() == Some(axis))
}

impl CoordinateSystem {
    pub(super) fn nonconsecutive_maps(&self) -> Option<&AxisMaps> {
        self.maps.as_ref().filter(|maps| !maps.concatenated)
    }

    /// Reads a casacore record in coordinate creation order, retaining
    /// separate pixel and world maps and each displaced axis's replacement.
    ///
    /// # Errors
    /// A model, axis permutation, replacement vector or observation record is invalid.
    pub fn from_mapped_record(record: &RecordValue) -> Result<Self, CoordinateError> {
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
        for (record_index, _, model) in models {
            if record_index > i32::MAX as usize {
                return Err(CoordinateError::InvalidRecord(
                    "coordinate number is too large".into(),
                ));
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
            placements.push(Placement {
                record_index,
                pixel,
                world,
            });
            coordinates.push(model);
        }
        let maps = AxisMaps::from_placements(placements)?;
        Ok(Self {
            coordinates,
            obs_info,
            maps: Some(maps),
        })
    }

    /// Converts a matrix of pixel coordinates, one point per column.
    ///
    /// # Errors
    /// The input has the wrong number of pixel rows or a point cannot be converted.
    pub fn to_world_columns(
        &self,
        pixel: ArrayView2<'_, f64>,
    ) -> Result<Array2<f64>, CoordinateError> {
        if pixel.nrows() != self.n_pixel_axes() {
            return Err(CoordinateError::DimensionMismatch {
                expected: self.n_pixel_axes(),
                got: pixel.nrows(),
            });
        }
        let mut result = Array2::zeros((self.n_world_axes(), pixel.ncols()).f());
        let mut buffer = Vec::with_capacity(pixel.nrows());
        for (column, mut world) in pixel.axis_iter(Axis(1)).zip(result.axis_iter_mut(Axis(1))) {
            let column = if let Some(data) = column.as_slice() {
                data
            } else {
                buffer.clear();
                buffer.extend(column.iter().copied());
                &buffer
            };
            let values = self.to_world(column)?;
            world
                .iter_mut()
                .zip(values)
                .for_each(|(out, value)| *out = value);
        }
        Ok(result)
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
    use crate::{DirectionCoordinate, LinearCoordinate, Projection, ProjectionType};
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
}
