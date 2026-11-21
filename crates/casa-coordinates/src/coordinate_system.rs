// SPDX-License-Identifier: LGPL-3.0-or-later
//! Coordinate system: container of coordinates with axis mapping.
//!
//! [`CoordinateSystem`] is the top-level object that groups multiple
//! [`Coordinate`](crate::Coordinate) objects into a single system describing a multi-dimensional
//! data cube. Each coordinate retains its own local axis order. Separate
//! pixel and world maps place visible axes in the public vectors and supply
//! replacement values for removed axes, as in casacore `CoordinateSystem`.

mod edit;
mod layout;

use casa_types::{RecordValue, Value};
use ndarray::{Array2, ArrayView2, Axis, ShapeBuilder};

use crate::coordinate::{CoordinateModel, CoordinateType};
use crate::direction::DirectionCoordinate;
use crate::error::CoordinateError;
use crate::linear::LinearCoordinate;
use crate::obs_info::ObsInfo;
use crate::record_utils::get_optional_string;
use crate::spectral::SpectralCoordinate;
use crate::stokes::StokesCoordinate;
use crate::tabular::TabularCoordinate;

use layout::{Layout, Placement, visible_indices};

/// A collection of coordinates describing a multi-dimensional data cube.
///
/// Newly added coordinates append their axes. A system restored from a
/// record can permute pixel and world axes independently or hide local axes.
///
/// Corresponds to C++ `CoordinateSystem`.
#[derive(Debug, Clone)]
pub struct CoordinateSystem {
    coordinates: Vec<CoordinateModel>,
    layout: Option<Box<Layout>>,
    obs_info: ObsInfo,
}

impl CoordinateSystem {
    /// Creates an empty coordinate system with default observation info.
    pub fn new() -> Self {
        Self {
            coordinates: Vec::new(),
            layout: None,
            obs_info: ObsInfo::default(),
        }
    }

    /// Adds a coordinate to the system.
    ///
    /// The new coordinate's axes are appended after all existing axes.
    pub fn add_coordinate(&mut self, coord: impl Into<CoordinateModel>) {
        let model = coord.into();
        if let Some(layout) = &mut self.layout {
            layout
                .placements
                .push(Placement::nominal(&model, layout.n_pixel, layout.n_world));
            layout.n_pixel += model.n_pixel_axes();
            layout.n_world += model.n_world_axes();
        }
        self.coordinates.push(model);
    }

    /// Returns the number of coordinates in the system.
    pub fn n_coordinates(&self) -> usize {
        self.coordinates.len()
    }

    /// Returns a reference to the coordinate at the given index.
    ///
    /// # Panics
    ///
    /// Panics if `index >= n_coordinates()`.
    pub fn coordinate(&self, index: usize) -> &CoordinateModel {
        &self.coordinates[index]
    }

    /// Replaces the coordinate at `index` with `coord`, returning the one it
    /// replaces.
    ///
    /// # Panics
    ///
    /// Panics if `index >= n_coordinates()`, or if `coord` has a different
    /// number of pixel or world axes, which would move the axes of the
    /// coordinates after it.
    pub fn replace_coordinate(
        &mut self,
        index: usize,
        coord: impl Into<CoordinateModel>,
    ) -> CoordinateModel {
        let coord = coord.into();
        let old = &self.coordinates[index];
        assert_eq!(
            (coord.n_pixel_axes(), coord.n_world_axes()),
            (old.n_pixel_axes(), old.n_world_axes()),
            "a replacing coordinate has the same axes"
        );
        if let Some(layout) = &mut self.layout {
            layout.placements[index].replace_model(old, &coord);
        }
        std::mem::replace(&mut self.coordinates[index], coord)
    }

    /// Returns the total number of pixel axes across all coordinates.
    pub fn n_pixel_axes(&self) -> usize {
        self.layout.as_ref().map_or_else(
            || self.coordinates.iter().map(|c| c.n_pixel_axes()).sum(),
            |layout| layout.n_pixel,
        )
    }

    /// Returns the total number of world axes across all coordinates.
    pub fn n_world_axes(&self) -> usize {
        self.layout.as_ref().map_or_else(
            || self.coordinates.iter().map(|c| c.n_world_axes()).sum(),
            |layout| layout.n_world,
        )
    }

    /// Finds the index of the first coordinate with the given type.
    ///
    /// Returns `None` if no coordinate of that type exists.
    pub fn find_coordinate(&self, coord_type: CoordinateType) -> Option<usize> {
        self.coordinates
            .iter()
            .position(|c| c.coordinate_type() == coord_type)
    }

    /// Returns a reference to the observation info.
    pub fn obs_info(&self) -> &ObsInfo {
        &self.obs_info
    }

    /// Returns a mutable reference to the observation info.
    pub fn obs_info_mut(&mut self) -> &mut ObsInfo {
        &mut self.obs_info
    }

    /// Sets the observation info. Returns `self` for chaining.
    pub fn with_obs_info(mut self, obs_info: ObsInfo) -> Self {
        self.obs_info = obs_info;
        self
    }

    /// Converts a full pixel coordinate vector to world coordinates.
    ///
    /// The pixel vector must have length [`n_pixel_axes`](Self::n_pixel_axes).
    /// Removed pixel axes use the replacements carried in the system.
    pub fn to_world(&self, pixel: &[f64]) -> Result<Vec<f64>, CoordinateError> {
        let n_pix = self.n_pixel_axes();
        if pixel.len() != n_pix {
            return Err(CoordinateError::DimensionMismatch {
                expected: n_pix,
                got: pixel.len(),
            });
        }

        if let Some(layout) = self.layout.as_ref().filter(|layout| !layout.contiguous) {
            let mut world = vec![0.0; layout.n_world];
            for (model, placement) in self.coordinates.iter().zip(&layout.placements) {
                placement.to_world(model, pixel, &mut world)?;
            }
            return Ok(world);
        }
        let mut world = Vec::with_capacity(self.n_world_axes());
        let mut pix_offset = 0;

        for coord in &self.coordinates {
            let np = coord.n_pixel_axes();
            let pix_slice = &pixel[pix_offset..pix_offset + np];
            let w = coord.to_world(pix_slice)?;
            world.extend(w);
            pix_offset += np;
        }

        Ok(world)
    }

    /// Converts a full world coordinate vector to pixel coordinates.
    ///
    /// The world vector must have length [`n_world_axes`](Self::n_world_axes).
    pub fn to_pixel(&self, world: &[f64]) -> Result<Vec<f64>, CoordinateError> {
        self.project_pixels(world, false)
    }

    /// Converts world to pixel like [`Self::to_pixel`], using NaN for axes
    /// whose linear transform is singular, as CASA's public tool does.
    ///
    /// # Errors
    /// Returns other conversion errors and wrong world-axis counts.
    pub fn to_pixel_or_nan(&self, world: &[f64]) -> Result<Vec<f64>, CoordinateError> {
        self.project_pixels(world, true)
    }

    fn project_pixels(&self, world: &[f64], undefined: bool) -> Result<Vec<f64>, CoordinateError> {
        let n_world = self.n_world_axes();
        if world.len() != n_world {
            return Err(CoordinateError::DimensionMismatch {
                expected: n_world,
                got: world.len(),
            });
        }

        if let Some(layout) = self.layout.as_ref().filter(|layout| !layout.contiguous) {
            let mut pixel = vec![0.0; layout.n_pixel];
            for (model, placement) in self.coordinates.iter().zip(&layout.placements) {
                placement.to_pixel(model, world, &mut pixel, undefined)?;
            }
            return Ok(pixel);
        }
        let mut pixel = Vec::with_capacity(self.n_pixel_axes());
        let mut world_offset = 0;

        for coord in &self.coordinates {
            let nw = coord.n_world_axes();
            let world_slice = &world[world_offset..world_offset + nw];
            match coord.to_pixel(world_slice) {
                Ok(values) => pixel.extend(values),
                Err(CoordinateError::SingularTransform(_)) if undefined => {
                    pixel.extend(std::iter::repeat_n(f64::NAN, coord.n_pixel_axes()));
                }
                Err(error) => return Err(error),
            }
            world_offset += nw;
        }

        Ok(pixel)
    }

    /// Converts a matrix of world vectors, with one point per column.
    ///
    /// The rows follow the system's world axes. The result has one row per
    /// pixel axis and holds its columns contiguously.
    ///
    /// # Errors
    ///
    /// Returns an error if the number of rows differs from the world axis
    /// count or a coordinate cannot be converted.
    pub fn to_pixel_columns(
        &self,
        world: ArrayView2<'_, f64>,
    ) -> Result<Array2<f64>, CoordinateError> {
        self.project_pixel_columns(world, false)
    }

    /// Converts world columns to pixel columns with NaNs for singular axes.
    ///
    /// # Errors
    /// Returns other conversion errors and wrong world-axis counts.
    pub fn to_pixel_columns_or_nan(
        &self,
        world: ArrayView2<'_, f64>,
    ) -> Result<Array2<f64>, CoordinateError> {
        self.project_pixel_columns(world, true)
    }

    fn project_pixel_columns(
        &self,
        world: ArrayView2<'_, f64>,
        undefined: bool,
    ) -> Result<Array2<f64>, CoordinateError> {
        let n_world = self.n_world_axes();
        if world.nrows() != n_world {
            return Err(CoordinateError::DimensionMismatch {
                expected: n_world,
                got: world.nrows(),
            });
        }

        let mut pixels = Array2::zeros((self.n_pixel_axes(), world.ncols()).f());
        if let Some(layout) = self.layout.as_ref().filter(|layout| !layout.contiguous) {
            for (model, placement) in self.coordinates.iter().zip(&layout.placements) {
                placement.to_pixel_columns(model, world, &mut pixels, undefined)?;
            }
            return Ok(pixels);
        }
        let mut input = Vec::new();
        let mut world_offset = 0;
        let mut pixel_offset = 0;
        for coord in &self.coordinates {
            let next_world = world_offset + coord.n_world_axes();
            let next_pixel = pixel_offset + coord.n_pixel_axes();
            let world = world.slice_axis(Axis(0), (world_offset..next_world).into());
            let mut pixel = pixels.slice_axis_mut(Axis(0), (pixel_offset..next_pixel).into());
            if let CoordinateModel::Linear(linear) = coord {
                match linear.to_pixel_columns(world, pixel.view_mut()) {
                    Ok(()) => (),
                    Err(CoordinateError::SingularTransform(_)) if undefined => pixel.fill(f64::NAN),
                    Err(error) => return Err(error),
                }
            } else {
                for (column, mut output) in
                    world.axis_iter(Axis(1)).zip(pixel.axis_iter_mut(Axis(1)))
                {
                    input.clear();
                    input.extend(column.iter().copied());
                    match coord.to_pixel(&input) {
                        Ok(values) => {
                            for (output, value) in output.iter_mut().zip(values) {
                                *output = value;
                            }
                        }
                        Err(CoordinateError::SingularTransform(_)) if undefined => {
                            output.fill(f64::NAN)
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
            world_offset = next_world;
            pixel_offset = next_pixel;
        }
        Ok(pixels)
    }

    /// Serializes the coordinate system using casacore's canonical
    /// `CoordinateSystem::save()` layout.
    pub fn to_record(&self) -> RecordValue {
        let mut rec = self.obs_info.to_record();
        let mut next_world_axis = 0;
        let mut next_pixel_axis = 0;

        for (index, coord) in self.coordinates.iter().enumerate() {
            let basename = match coord.coordinate_type() {
                CoordinateType::Linear => "linear",
                CoordinateType::Direction => "direction",
                CoordinateType::Spectral => "spectral",
                CoordinateType::Stokes => "stokes",
                CoordinateType::Tabular => "tabular",
            };
            rec.upsert(
                format!("{basename}{index}"),
                Value::Record(coord.to_record()),
            );

            if let Some(layout) = &self.layout {
                layout.placements[index].write(&mut rec, index);
            } else {
                Placement::nominal(coord, next_pixel_axis, next_world_axis).write(&mut rec, index);
                next_world_axis += coord.n_world_axes();
                next_pixel_axis += coord.n_pixel_axes();
            }
        }

        rec
    }

    /// Deserializes a coordinate system from a casacore-compatible record.
    ///
    /// Unknown or malformed coordinate sub-records are rejected; callers never
    /// receive a silently truncated coordinate system.
    ///
    /// Coordinate creation order and signed axis maps are preserved. Both
    /// public axis vectors are numbered independently from zero. A record
    /// without axis maps uses the coordinate creation order for both.
    ///
    /// # Errors
    /// Returns an error for malformed records, inconsistent maps or invalid
    /// replacements for removed axes.
    pub fn from_record(rec: &RecordValue) -> Result<Self, CoordinateError> {
        Self::restore_record(rec, false)
    }

    /// Restore a record from a live image operation, retaining sampled axes'
    /// live references alongside the persisted public axis maps.
    ///
    /// # Errors
    /// Returns an error for an invalid record or live sampled reference.
    pub fn from_live_record(rec: &RecordValue) -> Result<Self, CoordinateError> {
        Self::restore_record(rec, true)
    }

    fn restore_record(rec: &RecordValue, live: bool) -> Result<Self, CoordinateError> {
        let obs_info = match rec.get("obsinfo") {
            Some(Value::Record(obs_rec)) => ObsInfo::from_record(obs_rec)?,
            _ => ObsInfo::from_record(rec)?,
        };
        let (placed, layout) = read_coordinates(rec)?;
        let mut coordinates = Vec::with_capacity(placed.len());
        for (placement, mut coordinate) in placed {
            if live && let Some(Value::Record(stored)) = rec.get(&placement.field) {
                coordinate = match coordinate {
                    CoordinateModel::Spectral(spectral) if !spectral.has_linear_mapping() => {
                        spectral.with_live_reference(stored)?.into()
                    }
                    CoordinateModel::Tabular(table) => table.with_live_reference(stored)?.into(),
                    other => other,
                };
            }
            coordinates.push(coordinate);
        }
        Ok(Self {
            coordinates,
            layout: layout.map(Box::new),
            obs_info,
        })
    }

    /// Describes a record's coordinates and their local-to-public maps, in
    /// the same creation order as [`Self::from_record`].
    ///
    /// # Errors
    /// Returns the same record and map errors as [`Self::from_record`].
    pub fn record_coordinates(rec: &RecordValue) -> Result<Vec<RecordCoordinate>, CoordinateError> {
        let (placed, _) = read_coordinates(rec)?;
        Ok(placed.into_iter().map(|(entry, _)| entry).collect())
    }

    /// Return the first spectral coordinate in the record's creation order.
    ///
    /// The field name is returned with the coordinate so callers can update
    /// the same record even when its axes have been transposed.
    ///
    /// # Errors
    ///
    /// Returns an error if a coordinate record is malformed or unsupported.
    pub fn first_spectral_in_record(
        rec: &RecordValue,
    ) -> Result<Option<(String, SpectralCoordinate)>, CoordinateError> {
        for (_, key) in coordinate_record_entries(rec) {
            let Some(Value::Record(coordinate)) = rec.get(&key) else {
                return Err(CoordinateError::InvalidRecord(format!(
                    "coordinate field {key:?} is not a record"
                )));
            };
            if let CoordinateModel::Spectral(spectral) = parse_coordinate_record(&key, coordinate)?
            {
                return Ok(Some((key, *spectral)));
            }
        }
        Ok(None)
    }

    /// Return the first direction coordinate in the record's creation order.
    ///
    /// # Errors
    ///
    /// Returns an error if a coordinate record is malformed.
    pub fn first_direction_in_record(
        rec: &RecordValue,
    ) -> Result<Option<(String, DirectionCoordinate)>, CoordinateError> {
        for (_, key) in coordinate_record_entries(rec) {
            let Some(Value::Record(coordinate)) = rec.get(&key) else {
                return Err(CoordinateError::InvalidRecord(format!(
                    "coordinate field {key:?} is not a record"
                )));
            };
            if let CoordinateModel::Direction(direction) =
                parse_coordinate_record(&key, coordinate)?
            {
                return Ok(Some((key, direction)));
            }
        }
        Ok(None)
    }
}

/// One coordinate of a casacore coordinate-system record, as
/// [`CoordinateSystem::record_coordinates`] places it in the system.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordCoordinate {
    /// The record field that holds the coordinate (`direction0`,
    /// `stokes1`, ...).
    pub field: String,
    /// For each of the record coordinate's own axes, in its order, the
    /// system pixel axis it lies on; `None` for an axis the record removed.
    pub pixel_axes: Vec<Option<usize>>,
    /// The corresponding placement in the independently ordered world vector.
    pub world_axes: Vec<Option<usize>>,
}

impl RecordCoordinate {
    /// Read this coordinate before applying the system's pixel and world maps.
    /// This retains descriptors for axes that the maps remove.
    ///
    /// # Errors
    /// Returns an error if the field is missing or its coordinate is malformed.
    pub fn model(&self, record: &RecordValue) -> Result<CoordinateModel, CoordinateError> {
        let Some(Value::Record(coordinate)) = record.get(&self.field) else {
            return Err(CoordinateError::InvalidRecord(format!(
                "coordinate field {:?} is not a record",
                self.field
            )));
        };
        parse_coordinate_record(&self.field, coordinate)
    }
}

type PlacedCoordinates = Vec<(RecordCoordinate, CoordinateModel)>;

fn read_coordinates(
    rec: &RecordValue,
) -> Result<(PlacedCoordinates, Option<Layout>), CoordinateError> {
    let entries = coordinate_record_entries(rec);
    let explicit = entries.iter().any(|(index, _)| {
        rec.get(&format!("pixelmap{index}")).is_some()
            || rec.get(&format!("worldmap{index}")).is_some()
    });
    let mut coordinates = Vec::with_capacity(entries.len());
    let mut placements = Vec::with_capacity(if explicit { entries.len() } else { 0 });
    let (mut next_pixel, mut next_world) = (0, 0);
    for (index, field) in entries {
        let Some(Value::Record(coord_rec)) = rec.get(&field) else {
            return Err(CoordinateError::InvalidRecord(format!(
                "coordinate field '{field}' is not a record"
            )));
        };
        let model = parse_coordinate_record(&field, coord_rec)?;
        let end_pixel = next_pixel + model.n_pixel_axes();
        let end_world = next_world + model.n_world_axes();
        let (pixel_axes, world_axes) = if explicit {
            let placement = Placement::read(rec, index, &model)?;
            let axes = (
                visible_indices(&placement.pixel.indices),
                visible_indices(&placement.world.indices),
            );
            placements.push(placement);
            axes
        } else {
            (
                (next_pixel..end_pixel).map(Some).collect(),
                (next_world..end_world).map(Some).collect(),
            )
        };
        coordinates.push((
            RecordCoordinate {
                field,
                pixel_axes,
                world_axes,
            },
            model,
        ));
        next_pixel = end_pixel;
        next_world = end_world;
    }
    let layout = explicit
        .then(|| Layout::from_placements(placements))
        .transpose()?;
    Ok((coordinates, layout))
}

impl Default for CoordinateSystem {
    fn default() -> Self {
        Self::new()
    }
}

fn coordinate_record_entries(rec: &RecordValue) -> Vec<(usize, String)> {
    let mut modern = rec
        .fields()
        .iter()
        .filter_map(|field| {
            field
                .name
                .strip_prefix("coordinate")?
                .parse::<usize>()
                .ok()
                .map(|index| (index, field.name.clone()))
        })
        .collect::<Vec<_>>();
    modern.sort_unstable_by_key(|(index, _)| *index);
    modern.dedup_by_key(|(index, _)| *index);
    if !modern.is_empty() {
        return modern;
    }

    let mut legacy = rec
        .fields()
        .iter()
        .filter_map(|field| {
            legacy_coordinate_prefix(&field.name).and_then(|prefix| {
                field.name[prefix.len()..]
                    .parse::<usize>()
                    .ok()
                    .map(|index| (index, field.name.clone()))
            })
        })
        .collect::<Vec<_>>();
    legacy.sort_unstable_by_key(|(index, _)| *index);
    legacy.dedup_by_key(|(index, _)| *index);
    legacy
}

fn legacy_coordinate_prefix(name: &str) -> Option<&'static str> {
    ["direction", "spectral", "stokes", "linear", "tabular"]
        .into_iter()
        .find(|prefix| name.starts_with(prefix))
}

fn inferred_coordinate_type(name: &str) -> Option<&'static str> {
    let prefix = legacy_coordinate_prefix(name)?;
    match prefix {
        "linear" => Some("Linear"),
        "direction" => Some("Direction"),
        "spectral" => Some("Spectral"),
        "stokes" => Some("Stokes"),
        "tabular" => Some("Tabular"),
        _ => None,
    }
}

fn parse_coordinate_record(
    name: &str,
    rec: &RecordValue,
) -> Result<CoordinateModel, CoordinateError> {
    let coord_type = get_optional_string(rec, "coordinate_type")
        .or_else(|| inferred_coordinate_type(name).map(str::to_string))
        .ok_or_else(|| {
            CoordinateError::InvalidRecord(format!(
                "coordinate '{name}' has no recognized coordinate type"
            ))
        })?;
    match coord_type.as_str() {
        "Linear" => LinearCoordinate::from_record(rec).map(CoordinateModel::Linear),
        "Direction" => DirectionCoordinate::from_record(rec).map(CoordinateModel::Direction),
        "Spectral" => SpectralCoordinate::from_record(rec).map(Into::into),
        "Stokes" => StokesCoordinate::from_record(rec).map(CoordinateModel::Stokes),
        "Tabular" => TabularCoordinate::from_record(rec).map(CoordinateModel::Tabular),
        other => Err(CoordinateError::InvalidRecord(format!(
            "unsupported coordinate type '{other}' in '{name}'"
        ))),
    }
}

#[cfg(test)]
mod tests;
