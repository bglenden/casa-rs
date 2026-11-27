// SPDX-License-Identifier: LGPL-3.0-or-later

//! Coordinate projection and section metadata helpers for copying image regions (`ia.fromimage`, `ia.newimagefromimage`).

use casa_coordinates::TabularEdit;
use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, SpectralCoordinate,
    StokesCoordinate, TabularCoordinate,
};
use casa_core::quanta::{Quantity, Unit};
use casa_core::{ArrayValue, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, Axis};

use crate::analysis::{
    ImageRegionSection, reduce_subimage_coordinate_record, section_quality_labels,
};

use super::error::{ImageToolError, ImageToolResult};

/// Pixel-box and degenerate-axis projection describing an image region copy target.
pub struct Selection {
    /// Start index along each input pixel axis.
    pub start: Vec<usize>,
    /// Number of selected pixels along each input pixel axis.
    pub shape: Vec<usize>,
    /// Stride along each input pixel axis.
    pub stride: Vec<usize>,
    retained: Vec<usize>,
}

impl Selection {
    /// Initializes a copy selection covering all axes of `section`.
    pub fn new(section: &ImageRegionSection) -> Self {
        let rank = section.count.len();
        Self {
            start: section.start.clone(),
            shape: section.count.clone(),
            stride: section.stride.clone(),
            retained: (0..rank).collect(),
        }
    }

    /// Drops singleton axes of length `1`, matching CASA's `dropdeg=True` error if none remain.
    pub fn drop_degenerate(&mut self) -> ImageToolResult<()> {
        self.retained.retain(|&axis| self.shape[axis] > 1);
        if self.retained.is_empty() {
            return Err(ImageToolError::new(
                "LatticeStepper::setCursorShape: cursorShape has no axes or more axes than lattice",
            ));
        }
        Ok(())
    }

    /// Extracts entries from `values` corresponding to retained output axes.
    pub fn projected(&self, values: &[usize]) -> Vec<usize> {
        self.retained.iter().map(|&axis| values[axis]).collect()
    }

    /// Squeezes dropped degenerate axes out of `array`.
    pub fn squeeze<T>(&self, mut array: ArrayD<T>) -> ArrayD<T> {
        for axis in (0..self.shape.len()).rev() {
            if !self.retained.contains(&axis) {
                array = array.index_axis_move(Axis(axis), 0);
            }
        }
        array
    }

    /// Updates a casacore coordinate `record` to match the cropped, strided, and projected axes.
    pub fn coordinates(
        &self,
        mut record: RecordValue,
        original: &CoordinateSystem,
        image_shape: &[usize],
    ) -> ImageToolResult<RecordValue> {
        for (axis, &source_length) in image_shape.iter().enumerate() {
            let (number, within_coord) = original
                .pixel_axis_owner(axis)
                .ok_or_else(|| ImageToolError::new("Image section pixel axis has no coordinate"))?;
            let coord = original.coordinate(number);
            let kind = coord.coordinate_type();
            let shift = self.start[axis];
            let step = self.stride[axis];
            if let CoordinateModel::Stokes(stokes) = coord {
                if shift != 0 || step != 1 || self.shape[axis] != stokes.stokes().len() {
                    self.set_stokes(&mut record, stokes, axis)?;
                }
            } else if let CoordinateModel::Quality(quality) = coord {
                if shift != 0 || step != 1 || self.shape[axis] != quality.values().len() {
                    let field = CoordinateSystem::record_coordinates(&record)?
                        .into_iter()
                        .find(|coordinate| coordinate.pixel_axes.contains(&Some(axis)))
                        .ok_or_else(|| {
                            ImageToolError::new(
                                "Quality coordinate is missing from the image record",
                            )
                        })?
                        .field;
                    let Some(Value::Record(quality)) = record.get_mut(&field) else {
                        return Err(ImageToolError::new(
                            "Quality coordinate is missing from the image record",
                        ));
                    };
                    section_quality_labels(quality, shift, self.shape[axis], step)?;
                }
            } else if shift != 0 || step != 1 || self.shape[axis] != source_length {
                if kind == CoordinateType::Spectral
                    && self.crop_tabular_spectral(&mut record, axis)?
                {
                    continue;
                }
                if shift == 0 && step == 1 {
                    continue;
                }
                if let CoordinateModel::Tabular(table) = coord {
                    self.section_tabular(&mut record, table, axis)?;
                    continue;
                }
                let cdelt = coord.increment()[within_coord];
                // ImageInterface::subImage scales each coordinate's
                // increment and leaves its linear transform in place.
                let (_, stored_increment) =
                    CoordinateSystem::set_increment_in_record(&mut record, axis, cdelt)?;
                CoordinateSystem::set_increment_in_record(
                    &mut record,
                    axis,
                    stored_increment * step as f64,
                )?;
                let reference = coord.reference_pixel()[within_coord];
                CoordinateSystem::set_reference_pixel_in_record(
                    &mut record,
                    axis,
                    (reference - shift as f64) / step as f64,
                )?;
            }
        }
        if self.retained.len() == self.shape.len() {
            return Ok(record);
        }
        self.project_coordinates(&record)
    }

    fn section_tabular(
        &self,
        record: &mut RecordValue,
        table: &TabularCoordinate,
        axis: usize,
    ) -> ImageToolResult<()> {
        let mut table = table.clone();
        let shift = self.start[axis];
        let step = self.stride[axis];
        // casacore sets the reference pixel before scaling the increment.
        let reference = (table.reference_pixel()[0] - shift as f64) / step as f64;
        let increment = table.increment()[0] * step as f64;
        table.edit_live_reference(TabularEdit::ReferencePixel(reference))?;
        if step != 1 {
            table.edit_live_reference(TabularEdit::Increment(increment))?;
        }
        let field = CoordinateSystem::record_coordinates(record)?
            .into_iter()
            .find(|coordinate| coordinate.pixel_axes.contains(&Some(axis)))
            .ok_or_else(|| {
                ImageToolError::new("Tabular coordinate is missing from the image record")
            })?
            .field;
        let Some(Value::Record(stored)) = record.get_mut(&field) else {
            return Err(ImageToolError::new(
                "Tabular coordinate is missing from the image record",
            ));
        };
        for parameter in CoordinateModel::Tabular(table).to_record().fields() {
            stored.upsert(parameter.name.clone(), parameter.value.clone());
        }
        Ok(())
    }

    fn crop_tabular_spectral(
        &self,
        record: &mut RecordValue,
        axis: usize,
    ) -> ImageToolResult<bool> {
        let field = CoordinateSystem::record_coordinates(record)?
            .into_iter()
            .find(|coordinate| coordinate.pixel_axes.contains(&Some(axis)))
            .ok_or_else(|| {
                ImageToolError::new("Spectral coordinate is missing from the image record")
            })?
            .field;
        let Some(Value::Record(spectral)) = record.get_mut(&field) else {
            return Ok(false);
        };
        Ok(SpectralCoordinate::subimage_tabular_record_with_stride(
            spectral,
            self.start[axis],
            self.shape[axis],
            self.stride[axis],
        )?)
    }

    fn project_coordinates(&self, record: &RecordValue) -> ImageToolResult<RecordValue> {
        let mut projected =
            reduce_subimage_coordinate_record(record, self.shape.len(), &self.retained)?;
        reorder_rebuilt_world_axes(&mut projected)?;
        let model = CoordinateSystem::from_record(&projected)?;
        let world_at_origin = model.to_world(&vec![0.0; self.retained.len()])?;
        let units = model.world_axis_units();
        for coordinate in model.coordinates_in_record(&projected)? {
            let suffix = coordinate
                .field
                .trim_start_matches(|ch: char| !ch.is_ascii_digit());
            let pixel_field = format!("pixelreplace{suffix}");
            let world_field = format!("worldreplace{suffix}");
            let mut pixel = numbers(&projected, &pixel_field)
                .unwrap_or_else(|| vec![0.0; coordinate.pixel_axes.len()]);
            let mut world = numbers(&projected, &world_field)
                .unwrap_or_else(|| vec![0.0; coordinate.world_axes.len()]);
            pixel.resize(coordinate.pixel_axes.len(), 0.0);
            world.resize(coordinate.world_axes.len(), 0.0);
            let definition = projected
                .get(&coordinate.field)
                .expect("coordinate record entry exists");
            for (local, axis) in coordinate.pixel_axes.into_iter().enumerate() {
                if axis.is_some() {
                    pixel[local] = 0.0;
                }
            }
            for (local, axis) in coordinate.world_axes.into_iter().enumerate() {
                if let Some(axis) = axis {
                    world[local] =
                        world_value(world_at_origin[axis], &units[axis], definition, local)?;
                }
            }
            projected.upsert(pixel_field, ArrayValue::from_f64_vec(pixel).into());
            projected.upsert(world_field, ArrayValue::from_f64_vec(world).into());
        }
        Ok(projected)
    }

    fn set_stokes(
        &self,
        record: &mut RecordValue,
        stokes: &StokesCoordinate,
        axis: usize,
    ) -> ImageToolResult<()> {
        let stored = CoordinateSystem::record_coordinates(record)?
            .into_iter()
            .find(|coordinate| coordinate.pixel_axes.contains(&Some(axis)))
            .ok_or_else(|| {
                ImageToolError::new("Stokes coordinate is missing from the image record")
            })?;
        let values = (0..self.shape[axis])
            .map(|index| {
                stokes
                    .stokes()
                    .get(self.start[axis] + index * self.stride[axis])
                    .copied()
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                ImageToolError::new("Stokes coordinate does not cover the selected pixels")
            })?;
        record.upsert(
            stored.field,
            Value::Record(CoordinateModel::from(StokesCoordinate::new(values)).to_record()),
        );
        Ok(())
    }
}

fn reorder_rebuilt_world_axes(record: &mut RecordValue) -> ImageToolResult<()> {
    let coordinates = CoordinateSystem::from_mapped_record(record)?;
    if (0..coordinates.n_pixel_axes())
        .all(|pixel| coordinates.pixel_axis_to_world_axis(pixel) == Some(pixel))
    {
        return Ok(());
    }
    let placements = coordinates.coordinates_in_record(record)?;
    // The copy factories rebuild local coordinates in creation order and
    // transpose their world rows by the indices they owned in the section.
    let mut output = vec![0; coordinates.n_world_axes()];
    for (axis, world) in placements
        .iter()
        .flat_map(|item| item.world_axes.iter().flatten())
        .enumerate()
    {
        output[*world] = axis as i32;
    }
    let mut packed = 0;
    for placement in placements {
        let suffix = placement
            .field
            .trim_start_matches(|ch: char| !ch.is_ascii_digit());
        let key = format!("worldmap{suffix}");
        let Some(Value::Array(ArrayValue::Int32(axes))) = record.get_mut(&key) else {
            return Err(ImageToolError::new("Missing reduced world axis map"));
        };
        for axis in axes.iter_mut().filter(|axis| **axis >= 0) {
            *axis = output[packed];
            packed += 1;
        }
    }
    Ok(())
}

fn world_value(value: f64, unit: &str, definition: &Value, axis: usize) -> ImageToolResult<f64> {
    let Value::Record(definition) = definition else {
        return Ok(value);
    };
    let target = match definition.get("units") {
        Some(Value::Array(ArrayValue::String(units))) => units.iter().nth(axis).map(String::as_str),
        _ => match definition.get("unit") {
            Some(Value::Scalar(ScalarValue::String(unit))) => Some(unit.as_str()),
            _ => None,
        },
    };
    if let Some(target) = target.filter(|&target| target != unit) {
        let quantity = Quantity::new(value, unit)?;
        return Ok(quantity.get_value_in(&Unit::new(target)?)?);
    }
    Ok(value)
}

fn numbers(record: &RecordValue, key: &str) -> Option<Vec<f64>> {
    match record.get(key)? {
        Value::Array(ArrayValue::Float64(arr)) => Some(arr.t().iter().copied().collect()),
        Value::Array(ArrayValue::Float32(arr)) => {
            Some(arr.t().iter().map(|&v| f64::from(v)).collect())
        }
        Value::Array(ArrayValue::Int32(arr)) => {
            Some(arr.t().iter().map(|&v| f64::from(v)).collect())
        }
        Value::Array(ArrayValue::Int64(arr)) => Some(arr.t().iter().map(|&v| v as f64).collect()),
        _ => None,
    }
}
