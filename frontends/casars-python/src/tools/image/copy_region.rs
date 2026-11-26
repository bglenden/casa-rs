// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::TabularEdit;
use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, SpectralCoordinate,
    StokesCoordinate, TabularCoordinate,
};
use casa_core::quanta::{Quantity, Unit};
use casa_core::{ArrayValue, RecordValue, ScalarValue, Value};
use casa_imageanalysis::analysis::{
    ImageRegionSection, reduce_subimage_coordinate_record, section_quality_labels,
};
use ndarray::{ArrayD, Axis};
use pyo3::prelude::*;

use crate::convert::value::CasaValue;
use crate::tools::table::values;

use super::handle::raised;

pub(super) struct Selection {
    pub start: Vec<usize>,
    pub shape: Vec<usize>,
    pub stride: Vec<usize>,
    retained: Vec<usize>,
}

impl Selection {
    pub fn new(section: &ImageRegionSection) -> Self {
        let rank = section.count.len();
        Self {
            start: section.start.clone(),
            shape: section.count.clone(),
            stride: section.stride.clone(),
            retained: (0..rank).collect(),
        }
    }

    pub fn drop_degenerate(&mut self) -> PyResult<()> {
        self.retained.retain(|&axis| self.shape[axis] > 1);
        if self.retained.is_empty() {
            return Err(raised(
                "LatticeStepper::setCursorShape: cursorShape has no axes or more axes than lattice",
            ));
        }
        Ok(())
    }

    pub fn projected(&self, values: &[usize]) -> Vec<usize> {
        self.retained.iter().map(|&axis| values[axis]).collect()
    }

    pub fn squeeze<T>(&self, mut array: ArrayD<T>) -> ArrayD<T> {
        for axis in (0..self.shape.len()).rev() {
            if !self.retained.contains(&axis) {
                array = array.index_axis_move(Axis(axis), 0);
            }
        }
        array
    }

    pub fn coordinates(
        &self,
        mut record: RecordValue,
        original: &CoordinateSystem,
        image_shape: &[usize],
    ) -> PyResult<RecordValue> {
        for (axis, &source_length) in image_shape.iter().enumerate() {
            let (number, within_coord) = original
                .pixel_axis_owner(axis)
                .ok_or_else(|| raised("Image section pixel axis has no coordinate"))?;
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
                    let field = CoordinateSystem::record_coordinates(&record)
                        .map_err(raised)?
                        .into_iter()
                        .find(|coordinate| coordinate.pixel_axes.contains(&Some(axis)))
                        .ok_or_else(|| {
                            raised("Quality coordinate is missing from the image record")
                        })?
                        .field;
                    let Some(Value::Record(quality)) = record.get_mut(&field) else {
                        return Err(raised(
                            "Quality coordinate is missing from the image record",
                        ));
                    };
                    section_quality_labels(quality, shift, self.shape[axis], step)
                        .map_err(raised)?;
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
                    CoordinateSystem::set_increment_in_record(&mut record, axis, cdelt)
                        .map_err(raised)?;
                CoordinateSystem::set_increment_in_record(
                    &mut record,
                    axis,
                    stored_increment * step as f64,
                )
                .map_err(raised)?;
                let reference = coord.reference_pixel()[within_coord];
                CoordinateSystem::set_reference_pixel_in_record(
                    &mut record,
                    axis,
                    (reference - shift as f64) / step as f64,
                )
                .map_err(raised)?;
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
    ) -> PyResult<()> {
        let mut table = table.clone();
        let shift = self.start[axis];
        let step = self.stride[axis];
        // casacore sets the reference pixel before scaling the increment.
        let reference = (table.reference_pixel()[0] - shift as f64) / step as f64;
        let increment = table.increment()[0] * step as f64;
        table
            .edit_live_reference(TabularEdit::ReferencePixel(reference))
            .map_err(raised)?;
        if step != 1 {
            table
                .edit_live_reference(TabularEdit::Increment(increment))
                .map_err(raised)?;
        }
        let field = CoordinateSystem::record_coordinates(record)
            .map_err(raised)?
            .into_iter()
            .find(|coordinate| coordinate.pixel_axes.contains(&Some(axis)))
            .ok_or_else(|| raised("Tabular coordinate is missing from the image record"))?
            .field;
        let Some(Value::Record(stored)) = record.get_mut(&field) else {
            return Err(raised(
                "Tabular coordinate is missing from the image record",
            ));
        };
        for parameter in CoordinateModel::Tabular(table).to_record().fields() {
            stored.upsert(parameter.name.clone(), parameter.value.clone());
        }
        Ok(())
    }

    fn crop_tabular_spectral(&self, record: &mut RecordValue, axis: usize) -> PyResult<bool> {
        let field = CoordinateSystem::record_coordinates(record)
            .map_err(raised)?
            .into_iter()
            .find(|coordinate| coordinate.pixel_axes.contains(&Some(axis)))
            .ok_or_else(|| raised("Spectral coordinate is missing from the image record"))?
            .field;
        let Some(Value::Record(spectral)) = record.get_mut(&field) else {
            return Ok(false);
        };
        SpectralCoordinate::subimage_tabular_record_with_stride(
            spectral,
            self.start[axis],
            self.shape[axis],
            self.stride[axis],
        )
        .map_err(raised)
    }

    fn project_coordinates(&self, record: &RecordValue) -> PyResult<RecordValue> {
        let mut projected =
            reduce_subimage_coordinate_record(record, self.shape.len(), &self.retained)
                .map_err(raised)?;
        reorder_rebuilt_world_axes(&mut projected)?;
        let model = CoordinateSystem::from_record(&projected).map_err(raised)?;
        let world_at_origin = model
            .to_world(&vec![0.0; self.retained.len()])
            .map_err(raised)?;
        let units = model.world_axis_units();
        for coordinate in model.coordinates_in_record(&projected).map_err(raised)? {
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
    ) -> PyResult<()> {
        let stored = CoordinateSystem::record_coordinates(record)
            .map_err(raised)?
            .into_iter()
            .find(|coordinate| coordinate.pixel_axes.contains(&Some(axis)))
            .ok_or_else(|| raised("Stokes coordinate is missing from the image record"))?;
        let values = (0..self.shape[axis])
            .map(|index| {
                stokes
                    .stokes()
                    .get(self.start[axis] + index * self.stride[axis])
                    .copied()
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| raised("Stokes coordinate does not cover the selected pixels"))?;
        record.upsert(
            stored.field,
            Value::Record(CoordinateModel::from(StokesCoordinate::new(values)).to_record()),
        );
        Ok(())
    }
}

fn reorder_rebuilt_world_axes(record: &mut RecordValue) -> PyResult<()> {
    let coordinates = CoordinateSystem::from_mapped_record(record).map_err(raised)?;
    if (0..coordinates.n_pixel_axes())
        .all(|pixel| coordinates.pixel_axis_to_world_axis(pixel) == Some(pixel))
    {
        return Ok(());
    }
    let placements = coordinates.coordinates_in_record(record).map_err(raised)?;
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
            return Err(raised("Missing reduced world axis map"));
        };
        for axis in axes.iter_mut().filter(|axis| **axis >= 0) {
            *axis = output[packed];
            packed += 1;
        }
    }
    Ok(())
}

fn world_value(value: f64, unit: &str, definition: &Value, axis: usize) -> PyResult<f64> {
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
        let quantity = Quantity::new(value, unit).map_err(raised)?;
        return quantity
            .get_value_in(&Unit::new(target).map_err(raised)?)
            .map_err(raised);
    }
    Ok(value)
}

fn numbers(record: &RecordValue, key: &str) -> Option<Vec<f64>> {
    match values::value(record.get(key)?, std::path::Path::new(".")) {
        CasaValue::Floats { values, .. } => Some(values),
        CasaValue::Ints { values, .. } => {
            Some(values.into_iter().map(|value| value as f64).collect())
        }
        _ => None,
    }
}
