// SPDX-License-Identifier: LGPL-3.0-or-later
//! casacore region records for `WCPolygon` and `WCUnion` of `WCPolygon`.
//!
//! The record layout follows C++ `WCPolygon::toRecord` and
//! `WCCompound::makeRecord`: `isRegion` is 2 (a world-coordinate region),
//! `name` is the class name, polygon vertices are quantum vectors `x` and `y`
//! on the one-relative `pixelAxes`, and a union holds its members as `*1`,
//! `*2`, … under `regions`, with their count in `regions.nr`.

use casa_coordinates::CoordinateSystem;
use casa_types::quanta::{Quantity, Unit};
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};

use super::PolygonRegion;
use super::geometry::validate_polygon;
use crate::error::ImageError;

const REGION_TYPE_LC: i32 = 1;
const REGION_TYPE_WC: i32 = 2;
const REGION_TYPE_ARRAY_SLICER: i32 = 3;
const REGION_ABSREL_ABS: i32 = 1;
const WCPOLYGON_NAME: &str = "WCPolygon";
const WCUNION_NAME: &str = "WCUnion";

/// Encodes a region as a `WCPolygon` (one polygon) or `WCUnion` record.
pub(super) fn encode(
    region: &PolygonRegion,
    coords: &CoordinateSystem,
) -> Result<RecordValue, ImageError> {
    region.check_axes(coords)?;
    match region.polygons.as_slice() {
        [] => Err(ImageError::InvalidMetadata(
            "a polygon region needs at least one polygon to be saved".into(),
        )),
        [polygon] => wc_polygon_record(region, polygon, coords),
        polygons => {
            let mut record = base_wc_region_record(WCUNION_NAME);
            let mut children = RecordValue::default();
            for (index, polygon) in polygons.iter().enumerate() {
                children.upsert(
                    format!("*{}", index + 1),
                    Value::Record(wc_polygon_record(region, polygon, coords)?),
                );
            }
            children.upsert(
                "nr",
                Value::Scalar(ScalarValue::Int32(polygons.len() as i32)),
            );
            record.upsert("regions", Value::Record(children));
            Ok(record)
        }
    }
}

/// Decodes the saved region `saved_name` from its record.
pub(super) fn decode(
    saved_name: &str,
    record: &RecordValue,
    coords: &CoordinateSystem,
) -> Result<PolygonRegion, ImageError> {
    let mut polygons = Vec::new();
    let mut pixel_axes = None;
    collect_polygons(saved_name, record, &mut pixel_axes, &mut polygons)?;
    let Some(pixel_axes) = pixel_axes else {
        return Err(ImageError::InvalidMetadata(format!(
            "saved region '{saved_name}' does not contain any polygons"
        )));
    };
    let mut region = PolygonRegion::new(coords, pixel_axes).map_err(|error| {
        ImageError::InvalidMetadata(format!("saved region '{saved_name}': {error}"))
    })?;
    for (vertices, [x_unit, y_unit]) in polygons {
        let x = vertices.iter().map(|vertex| vertex[0]).collect::<Vec<_>>();
        let y = vertices.iter().map(|vertex| vertex[1]).collect::<Vec<_>>();
        let x = convert_region_coordinate_values(&x, &x_unit, &region.units[0], saved_name, "x")?;
        let y = convert_region_coordinate_values(&y, &y_unit, &region.units[1], saved_name, "y")?;
        region.add_polygon(x.into_iter().zip(y).map(|(x, y)| [x, y]).collect())?;
    }
    Ok(region)
}

type UnitPolygon = (Vec<[f64; 2]>, [String; 2]);

fn collect_polygons(
    saved_name: &str,
    record: &RecordValue,
    pixel_axes: &mut Option<[usize; 2]>,
    polygons: &mut Vec<UnitPolygon>,
) -> Result<(), ImageError> {
    match native_region_kind(saved_name, record)? {
        WCPOLYGON_NAME => {
            let (axes, polygon) = parse_wc_polygon_record(saved_name, record)?;
            match pixel_axes {
                Some(existing) if *existing != axes => {
                    return Err(ImageError::InvalidMetadata(format!(
                        "saved region '{saved_name}' unites polygons on pixel axes {existing:?} and {axes:?}"
                    )));
                }
                _ => *pixel_axes = Some(axes),
            }
            polygons.push(polygon);
            Ok(())
        }
        WCUNION_NAME => {
            let Some(Value::Record(children)) = record.get("regions") else {
                return Err(ImageError::InvalidMetadata(format!(
                    "saved region '{saved_name}' is missing the regions subrecord"
                )));
            };
            let child_count = match children.get("nr") {
                Some(Value::Scalar(ScalarValue::Int32(value))) if *value >= 0 => *value as usize,
                Some(_) => {
                    return Err(ImageError::InvalidMetadata(format!(
                        "saved region '{saved_name}' has a non-integer regions.nr field"
                    )));
                }
                None => children.fields().len(),
            };
            for index in 0..child_count {
                let child = children
                    .get(&format!("*{}", index + 1))
                    .or_else(|| children.get(&index.to_string()));
                let Some(Value::Record(child)) = child else {
                    return Err(ImageError::InvalidMetadata(format!(
                        "saved region '{saved_name}' is missing child region {}",
                        index + 1
                    )));
                };
                collect_polygons(saved_name, child, pixel_axes, polygons)?;
            }
            Ok(())
        }
        other => Err(ImageError::InvalidMetadata(format!(
            "saved region '{saved_name}' uses unsupported casacore type {other}; only WCPolygon and WCUnion of WCPolygon regions are supported"
        ))),
    }
}

fn parse_wc_polygon_record(
    saved_name: &str,
    record: &RecordValue,
) -> Result<([usize; 2], UnitPolygon), ImageError> {
    let Some(Value::Scalar(ScalarValue::Bool(one_rel))) = record.get("oneRel") else {
        return Err(ImageError::InvalidMetadata(format!(
            "saved region '{saved_name}' is missing oneRel"
        )));
    };
    let Some(Value::Scalar(ScalarValue::Int32(absrel))) = record.get("absrel") else {
        return Err(ImageError::InvalidMetadata(format!(
            "saved region '{saved_name}' is missing absrel"
        )));
    };
    if *absrel != REGION_ABSREL_ABS {
        return Err(ImageError::InvalidMetadata(format!(
            "saved region '{saved_name}' uses unsupported absrel mode {absrel}; only absolute WCPolygon coordinates are supported"
        )));
    }

    let offset = i32::from(*one_rel);
    let pixel_axes = parse_i32_array_field(record, "pixelAxes", saved_name)?
        .into_iter()
        .map(|axis| usize::try_from(axis - offset).ok())
        .collect::<Option<Vec<_>>>();
    let Some(&[x_axis, y_axis]) = pixel_axes.as_deref() else {
        return Err(ImageError::InvalidMetadata(format!(
            "saved region '{saved_name}' has invalid polygon pixelAxes {pixel_axes:?}"
        )));
    };

    let Some(Value::Record(coords_record)) = record.get("coordinates") else {
        return Err(ImageError::InvalidMetadata(format!(
            "saved region '{saved_name}' is missing coordinates"
        )));
    };
    CoordinateSystem::from_record(coords_record).map_err(|error| {
        ImageError::InvalidMetadata(format!(
            "saved region '{saved_name}' has unreadable coordinates: {error}"
        ))
    })?;

    let (x_values, x_unit) = parse_quantum_vector_field(record, "x", saved_name)?;
    let (y_values, y_unit) = parse_quantum_vector_field(record, "y", saved_name)?;
    if x_values.len() != y_values.len() {
        return Err(ImageError::InvalidMetadata(format!(
            "saved region '{saved_name}' has mismatched polygon coordinate lengths"
        )));
    }
    let vertices = x_values
        .into_iter()
        .zip(y_values)
        .map(|(x, y)| [x, y])
        .collect();
    Ok(([x_axis, y_axis], (vertices, [x_unit, y_unit])))
}

fn wc_polygon_record(
    region: &PolygonRegion,
    polygon: &[[f64; 2]],
    coords: &CoordinateSystem,
) -> Result<RecordValue, ImageError> {
    validate_polygon(polygon)?;
    let mut record = base_wc_region_record(WCPOLYGON_NAME);
    record.upsert("oneRel", Value::Scalar(ScalarValue::Bool(true)));
    record.upsert(
        "pixelAxes",
        Value::Array(ArrayValue::from_i32_vec(vec![
            region.pixel_axes[0] as i32 + 1,
            region.pixel_axes[1] as i32 + 1,
        ])),
    );
    record.upsert(
        "x",
        Value::Record(quantum_vector_record(
            polygon.iter().map(|vertex| vertex[0]).collect(),
            &region.units[0],
        )),
    );
    record.upsert(
        "y",
        Value::Record(quantum_vector_record(
            polygon.iter().map(|vertex| vertex[1]).collect(),
            &region.units[1],
        )),
    );
    record.upsert(
        "absrel",
        Value::Scalar(ScalarValue::Int32(REGION_ABSREL_ABS)),
    );
    record.upsert("coordinates", Value::Record(coords.to_record()));
    Ok(record)
}

fn base_wc_region_record(class_name: &str) -> RecordValue {
    let mut record = RecordValue::default();
    record.upsert(
        "isRegion",
        Value::Scalar(ScalarValue::Int32(REGION_TYPE_WC)),
    );
    record.upsert(
        "name",
        Value::Scalar(ScalarValue::String(class_name.to_string())),
    );
    record.upsert("comment", Value::Scalar(ScalarValue::String(String::new())));
    record
}

fn quantum_vector_record(values: Vec<f64>, unit: &str) -> RecordValue {
    let mut record = RecordValue::default();
    record.upsert("value", Value::Array(ArrayValue::from_f64_vec(values)));
    record.upsert("unit", Value::Scalar(ScalarValue::String(unit.to_string())));
    record
}

fn native_region_kind<'a>(
    saved_name: &str,
    record: &'a RecordValue,
) -> Result<&'a str, ImageError> {
    let Some(Value::Scalar(ScalarValue::Int32(region_type))) = record.get("isRegion") else {
        return Err(ImageError::InvalidMetadata(format!(
            "saved region '{saved_name}' is missing isRegion"
        )));
    };
    let Some(Value::Scalar(ScalarValue::String(name))) = record.get("name") else {
        return Err(ImageError::InvalidMetadata(format!(
            "saved region '{saved_name}' is missing name"
        )));
    };
    match *region_type {
        REGION_TYPE_WC => Ok(name.as_str()),
        REGION_TYPE_LC => Err(ImageError::InvalidMetadata(format!(
            "saved region '{saved_name}' uses unsupported casacore type {name}; LC/pixel regions are not supported"
        ))),
        REGION_TYPE_ARRAY_SLICER => Err(ImageError::InvalidMetadata(format!(
            "saved region '{saved_name}' uses unsupported casacore type {name}; array-slicer regions are not supported"
        ))),
        other => Err(ImageError::InvalidMetadata(format!(
            "saved region '{saved_name}' uses unsupported casacore region type {other} ({name})"
        ))),
    }
}

fn parse_quantum_vector_field(
    record: &RecordValue,
    field_name: &str,
    saved_name: &str,
) -> Result<(Vec<f64>, String), ImageError> {
    let Some(Value::Record(quantum)) = record.get(field_name) else {
        return Err(ImageError::InvalidMetadata(format!(
            "saved region '{saved_name}' is missing {field_name}"
        )));
    };
    let values = parse_f64_array_field(quantum, "value", saved_name)?;
    let Some(Value::Scalar(ScalarValue::String(unit))) = quantum.get("unit") else {
        return Err(ImageError::InvalidMetadata(format!(
            "saved region '{saved_name}' has no unit for {field_name}"
        )));
    };
    Ok((values, unit.clone()))
}

fn convert_region_coordinate_values(
    values: &[f64],
    from_unit: &str,
    target_unit: &str,
    saved_name: &str,
    field_name: &str,
) -> Result<Vec<f64>, ImageError> {
    if from_unit == target_unit {
        return Ok(values.to_vec());
    }

    let target = Unit::new(target_unit).map_err(|error| {
        ImageError::InvalidMetadata(format!(
            "saved region '{saved_name}' targets unsupported unit '{target_unit}' for {field_name}: {error}"
        ))
    })?;

    values
        .iter()
        .copied()
        .map(|value| {
            let quantity = Quantity::new(value, from_unit).map_err(|error| {
                ImageError::InvalidMetadata(format!(
                    "saved region '{saved_name}' uses unsupported coordinate unit '{from_unit}' for {field_name}: {error}"
                ))
            })?;
            quantity.get_value_in(&target).map_err(|error| {
                ImageError::InvalidMetadata(format!(
                    "saved region '{saved_name}' uses incompatible coordinate unit '{from_unit}' for {field_name}; cannot convert to '{target_unit}': {error}"
                ))
            })
        })
        .collect()
}

fn parse_i32_array_field(
    record: &RecordValue,
    field_name: &str,
    saved_name: &str,
) -> Result<Vec<i32>, ImageError> {
    let Some(Value::Array(ArrayValue::Int32(values))) = record.get(field_name) else {
        return Err(ImageError::InvalidMetadata(format!(
            "saved region '{saved_name}' field {field_name} is not an Int32 array"
        )));
    };
    Ok(values.iter().copied().collect())
}

fn parse_f64_array_field(
    record: &RecordValue,
    field_name: &str,
    saved_name: &str,
) -> Result<Vec<f64>, ImageError> {
    let Some(Value::Array(ArrayValue::Float64(values))) = record.get(field_name) else {
        return Err(ImageError::InvalidMetadata(format!(
            "saved region '{saved_name}' field {field_name} is not a Float64 array"
        )));
    };
    Ok(values.iter().copied().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::f64::consts::PI;

    #[test]
    fn native_region_kind_reports_non_wc_classes_with_useful_errors() {
        let mut lc_record = RecordValue::default();
        lc_record.upsert("isRegion", Value::Scalar(ScalarValue::Int32(1)));
        lc_record.upsert(
            "name",
            Value::Scalar(ScalarValue::String("LCBox".to_string())),
        );
        let lc_error = native_region_kind("pixels", &lc_record).expect_err("LC regions fail");
        assert!(lc_error.to_string().contains("LC/pixel regions"));
        assert!(lc_error.to_string().contains("LCBox"));

        let mut slicer_record = RecordValue::default();
        slicer_record.upsert(
            "isRegion",
            Value::Scalar(ScalarValue::Int32(REGION_TYPE_ARRAY_SLICER)),
        );
        slicer_record.upsert(
            "name",
            Value::Scalar(ScalarValue::String("LCSlicer".to_string())),
        );
        let slicer_error =
            native_region_kind("slice", &slicer_record).expect_err("array slicers fail");
        assert!(slicer_error.to_string().contains("array-slicer regions"));
        assert!(slicer_error.to_string().contains("LCSlicer"));
    }

    #[test]
    fn convert_region_coordinate_values_converts_and_reports_unit_errors() {
        let radians = convert_region_coordinate_values(&[180.0], "deg", "rad", "demo", "x")
            .expect("convert degrees to radians");
        assert!((radians[0] - PI).abs() < 1e-12);

        let unsupported = convert_region_coordinate_values(&[1.0], "bogus", "rad", "demo", "x")
            .expect_err("unsupported source unit");
        assert!(
            unsupported
                .to_string()
                .contains("unsupported coordinate unit 'bogus'")
        );

        let incompatible = convert_region_coordinate_values(&[1.0], "deg", "m", "demo", "x")
            .expect_err("incompatible target unit");
        assert!(incompatible.to_string().contains("cannot convert to 'm'"));
    }

    #[test]
    fn parse_quantum_vector_field_requires_record_unit_and_float_values() {
        let mut missing_unit_quantum = RecordValue::default();
        missing_unit_quantum.upsert("value", Value::Array(ArrayValue::from_f64_vec(vec![1.0])));
        let mut missing_unit_record = RecordValue::default();
        missing_unit_record.upsert("x", Value::Record(missing_unit_quantum));
        let missing_unit = parse_quantum_vector_field(&missing_unit_record, "x", "demo")
            .expect_err("missing unit should fail");
        assert!(missing_unit.to_string().contains("has no unit for x"));

        let mut wrong_values_quantum = RecordValue::default();
        wrong_values_quantum.upsert("value", Value::Array(ArrayValue::from_i32_vec(vec![1, 2])));
        wrong_values_quantum.upsert(
            "unit",
            Value::Scalar(ScalarValue::String("deg".to_string())),
        );
        let mut wrong_values_record = RecordValue::default();
        wrong_values_record.upsert("x", Value::Record(wrong_values_quantum));
        let wrong_values = parse_quantum_vector_field(&wrong_values_record, "x", "demo")
            .expect_err("non-float values should fail");
        assert!(
            wrong_values
                .to_string()
                .contains("field value is not a Float64 array")
        );
    }
}
