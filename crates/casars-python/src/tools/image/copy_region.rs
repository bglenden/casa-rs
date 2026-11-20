// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{
    CoordinateModel, CoordinateSystem, CoordinateType, SpectralCoordinate, StokesCoordinate,
};
use casa_types::quanta::{Quantity, Unit};
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, Axis};
use pyo3::prelude::*;

use crate::convert::value::CasaValue;
use crate::tools::table::values;

use super::handle::raised;
use super::stub;

pub(super) struct Selection {
    pub start: Vec<usize>,
    pub shape: Vec<usize>,
    pub stride: Vec<usize>,
    retained: Vec<usize>,
}

impl Selection {
    pub fn new(image_shape: &[usize], region: &RecordValue, method: &str) -> PyResult<Self> {
        let rank = image_shape.len();
        if region.is_empty() {
            return Ok(Self {
                start: vec![0; rank],
                shape: image_shape.to_vec(),
                stride: vec![1; rank],
                retained: (0..rank).collect(),
            });
        }
        let supported_name = matches!(region.get("name"), Some(Value::Scalar(ScalarValue::String(name))) if name == "LCSlicer");
        let absolute = ["arblc", "artrc"].iter().all(|field| {
            numbers(region, field).is_some_and(|values| values.iter().all(|value| *value == 1.0))
        });
        let integral = ["fracblc", "fractrc", "fracinc"].iter().all(|field| {
            matches!(region.get(field), Some(Value::Array(ArrayValue::Bool(flags))) if flags.iter().all(|flag| !flag))
        });
        if !supported_name || !absolute || !integral {
            return Err(stub(method).unwrap_err());
        }
        let offset = usize::from(matches!(
            region.get("oneRel"),
            Some(Value::Scalar(ScalarValue::Bool(true)))
        ));
        let invalid = || raised("LCSlicer::makeComplete - invalid bounds or stride");
        let start = numbers(region, "blc").ok_or_else(invalid)?;
        let stop = numbers(region, "trc").ok_or_else(invalid)?;
        let stride = numbers(region, "inc").ok_or_else(invalid)?;
        if [start.len(), stop.len(), stride.len()]
            .into_iter()
            .any(|len| len > rank)
        {
            return Err(invalid());
        }
        let mut selection = Self {
            start: Vec::with_capacity(rank),
            shape: Vec::with_capacity(rank),
            stride: Vec::with_capacity(rank),
            retained: (0..rank).collect(),
        };
        let mut lower = Vec::with_capacity(rank);
        let mut upper = Vec::with_capacity(rank);
        for (axis, &extent) in image_shape.iter().enumerate() {
            lower.push(slicer_index(start.get(axis), offset, 0)?.max(0));
            upper.push(
                slicer_index(stop.get(axis), offset, extent as i64 - 1)?.min(extent as i64 - 1),
            );
            let step = slicer_index(stride.get(axis), 0, 1)?;
            if step <= 0 {
                return Err(raised("Slicer: stride must be positive"));
            }
            selection.stride.push(step as usize);
        }
        if lower.iter().zip(&upper).any(|(blc, trc)| blc > trc) {
            return Err(raised(format!(
                "LCSlicer::toSlicer - blc {lower:?} must be <= trc {upper:?}"
            )));
        }
        for ((lower, upper), stride) in lower.into_iter().zip(upper).zip(&selection.stride) {
            selection.start.push(lower as usize);
            selection.shape.push(1 + (upper - lower) as usize / stride);
        }
        Ok(selection)
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
        method: &str,
    ) -> PyResult<RecordValue> {
        let mut axes = 0;
        for number in 0..original.n_coordinates() {
            let coord = original.coordinate(number);
            let kind = coord.coordinate_type();
            let pixels = coord.reference_pixel();
            for (within_coord, &reference) in pixels.iter().enumerate() {
                let axis = axes + within_coord;
                let shift = self.start[axis];
                let step = self.stride[axis];
                if let CoordinateModel::Stokes(stokes) = coord {
                    if shift != 0 || step != 1 || self.shape[axis] != stokes.stokes().len() {
                        self.set_stokes(&mut record, stokes, axis)?;
                    }
                } else if shift != 0 || step != 1 || self.shape[axis] != image_shape[axis] {
                    if kind == CoordinateType::Spectral
                        && self.crop_tabular_spectral(&mut record, axis)?
                    {
                        continue;
                    }
                    if kind == CoordinateType::Tabular {
                        return Err(stub(method).unwrap_err());
                    }
                    if shift == 0 && step == 1 {
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
                    CoordinateSystem::set_reference_pixel_in_record(
                        &mut record,
                        axis,
                        (reference - shift as f64) / step as f64,
                    )
                    .map_err(raised)?;
                }
            }
            axes += coord.n_pixel_axes();
        }
        if self.retained.len() == self.shape.len() {
            return Ok(record);
        }
        self.project_coordinates(&record)
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
        let model = CoordinateSystem::from_record(record).map_err(raised)?;
        let world_at_origin = model
            .to_world(&vec![0.0; self.shape.len()])
            .map_err(raised)?;
        let units = (0..model.n_coordinates())
            .flat_map(|index| model.coordinate(index).axis_units())
            .collect::<Vec<_>>();
        let mut coordinates = CoordinateSystem::record_coordinates(record).map_err(raised)?;
        coordinates.sort_by_key(|coordinate| {
            let split = coordinate
                .field
                .trim_end_matches(|ch: char| ch.is_ascii_digit())
                .len();
            coordinate.field[split..].parse::<usize>().unwrap_or(0)
        });
        let mut projected = record.clone();
        let auxiliary = ["pixelmap", "worldmap", "pixelreplace", "worldreplace"];
        for coordinate in &coordinates {
            let split = coordinate
                .field
                .trim_end_matches(|ch: char| ch.is_ascii_digit())
                .len();
            let suffix = &coordinate.field[split..];
            projected.remove(&coordinate.field);
            for key in auxiliary {
                projected.remove(&format!("{key}{suffix}"));
            }
        }
        let mut index = 0;
        for coordinate in coordinates {
            let retained = coordinate
                .pixel_axes
                .iter()
                .flatten()
                .filter_map(|axis| self.retained.iter().position(|kept| kept == axis))
                .map(|axis| axis as i32)
                .collect::<Vec<_>>();
            if retained.is_empty() {
                continue;
            }
            if retained.len() != coordinate.pixel_axes.len() {
                return Err(raised(
                    "This subimage removes part of a coordinate (casa-rs issue #1002)",
                ));
            }
            let kind = coordinate
                .field
                .trim_end_matches(|ch: char| ch.is_ascii_digit());
            let definition = record
                .get(&coordinate.field)
                .expect("coordinate record entry exists");
            projected.upsert(format!("{kind}{index}"), definition.clone());
            for key in ["pixelmap", "worldmap"] {
                projected.upsert(
                    format!("{key}{index}"),
                    Value::Array(ArrayValue::from_i32_vec(retained.clone())),
                );
            }
            projected.upsert(
                format!("pixelreplace{index}"),
                Value::Array(ArrayValue::from_f64_vec(vec![0.0; retained.len()])),
            );
            let world = coordinate
                .pixel_axes
                .iter()
                .flatten()
                .enumerate()
                .map(|(local, &axis)| {
                    world_value(world_at_origin[axis], &units[axis], definition, local)
                })
                .collect::<PyResult<Vec<_>>>()?;
            projected.upsert(
                format!("worldreplace{index}"),
                Value::Array(ArrayValue::from_f64_vec(world)),
            );
            index += 1;
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
        let first = f64::from(values[0].code());
        record.upsert(
            format!("worldreplace{}", &stored.field["stokes".len()..]),
            Value::Array(ArrayValue::from_f64_vec(vec![first])),
        );
        record.upsert(
            stored.field,
            Value::Record(CoordinateModel::from(StokesCoordinate::new(values)).to_record()),
        );
        Ok(())
    }
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

fn slicer_index(input: Option<&f64>, offset: usize, default: i64) -> PyResult<i64> {
    // Slicer::MimicSource can be rounded by the float-precision record.
    let Some(&value) = input.filter(|&&v| v >= -2_147_483_636.0 || !v.is_finite()) else {
        return Ok(default);
    };
    if !value.is_finite() || value > i32::MAX as f64 {
        return Err(raised("LCSlicer::makeComplete - invalid pixel coordinate"));
    }
    Ok((value as f32 - offset as f32 + 0.5) as i64)
}
