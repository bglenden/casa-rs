// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn next_saved_region_name(existing: &[String]) -> String {
    let mut index = 1usize;
    loop {
        let candidate = format!("Region {index}");
        if !existing.iter().any(|name| name == &candidate) {
            return candidate;
        }
        index = index.saturating_add(1);
    }
}

pub(super) fn next_region_mask_name(existing: &[String]) -> String {
    let mut index = 1usize;
    loop {
        let candidate = format!("region_mask_{index}");
        if !existing.iter().any(|name| name == &candidate) {
            return candidate;
        }
        index = index.saturating_add(1);
    }
}

pub(super) fn save_native_region_record(
    path: &Path,
    pixel_type: ImagePixelType,
    name: &str,
    record: &RecordValue,
) -> Result<(), ImageError> {
    match pixel_type {
        ImagePixelType::Float32 => {
            let mut image = PagedImage::<f32>::open(path)?;
            image.put_region_record(name, record)?;
            image.save()
        }
        ImagePixelType::Float64 => {
            let mut image = PagedImage::<f64>::open(path)?;
            image.put_region_record(name, record)?;
            image.save()
        }
        ImagePixelType::Complex32 => {
            let mut image = PagedImage::<casa_types::Complex32>::open(path)?;
            image.put_region_record(name, record)?;
            image.save()
        }
        ImagePixelType::Complex64 => {
            let mut image = PagedImage::<casa_types::Complex64>::open(path)?;
            image.put_region_record(name, record)?;
            image.save()
        }
    }
}

pub(super) fn remove_native_region_record(
    path: &Path,
    pixel_type: ImagePixelType,
    name: &str,
) -> Result<(), ImageError> {
    match pixel_type {
        ImagePixelType::Float32 => {
            let mut image = PagedImage::<f32>::open(path)?;
            image.remove_region(name)?;
            image.save()
        }
        ImagePixelType::Float64 => {
            let mut image = PagedImage::<f64>::open(path)?;
            image.remove_region(name)?;
            image.save()
        }
        ImagePixelType::Complex32 => {
            let mut image = PagedImage::<casa_types::Complex32>::open(path)?;
            image.remove_region(name)?;
            image.save()
        }
        ImagePixelType::Complex64 => {
            let mut image = PagedImage::<casa_types::Complex64>::open(path)?;
            image.remove_region(name)?;
            image.save()
        }
    }
}

pub(super) fn rename_native_region_record_name(
    record: &RecordValue,
    new_name: &str,
) -> RecordValue {
    let mut renamed = record.clone();
    renamed.upsert(
        "comment",
        casa_types::Value::Scalar(casa_types::ScalarValue::String(new_name.to_string())),
    );
    if let Some(casa_types::Value::Record(regions)) = renamed.get_mut("regions") {
        for child_name in regions
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .collect::<Vec<_>>()
        {
            if child_name == "nr" {
                continue;
            }
            if let Some(casa_types::Value::Record(child)) = regions.get_mut(&child_name) {
                child.upsert(
                    "comment",
                    casa_types::Value::Scalar(casa_types::ScalarValue::String(
                        new_name.to_string(),
                    )),
                );
            }
        }
    }
    renamed
}

pub(super) fn region_to_native_record(
    view: &OpenedImageView,
    region: &ImageRegion,
) -> Result<RecordValue, ImageError> {
    let display_axes = view
        .axis_model()
        .display_axes
        .ok_or_else(|| ImageError::InvalidMetadata(view.status_line()))?;
    validate_region_axes(region, display_axes)?;
    if region.shapes.is_empty() {
        return Err(ImageError::InvalidMetadata(
            "no closed polygon shapes to save".into(),
        ));
    }
    if region.has_open_shape() {
        return Err(ImageError::InvalidMetadata(
            "close or cancel the current polygon before saving".into(),
        ));
    }

    let closed_shapes = region
        .shapes
        .iter()
        .filter(|shape| shape.closed)
        .cloned()
        .collect::<Vec<_>>();
    if closed_shapes.is_empty() {
        return Err(ImageError::InvalidMetadata(
            "no closed polygon shapes to save".into(),
        ));
    }

    for shape in &closed_shapes {
        validate_polygon_shape(shape)?;
    }

    if closed_shapes.len() == 1 {
        wc_polygon_record(view, region, &closed_shapes[0])
    } else {
        let mut record = base_wc_region_record(WCUNION_NAME);
        let mut children = RecordValue::default();
        for (index, shape) in closed_shapes.iter().enumerate() {
            children.upsert(
                format!("*{}", index + 1),
                Value::Record(wc_polygon_record(view, region, shape)?),
            );
        }
        children.upsert(
            "nr",
            Value::Scalar(ScalarValue::Int32(closed_shapes.len() as i32)),
        );
        record.upsert("regions", Value::Record(children));
        Ok(record)
    }
}

pub(super) fn region_from_native_record(
    view: &OpenedImageView,
    saved_name: &str,
    record: &RecordValue,
) -> Result<ImageRegion, ImageError> {
    let mut region = view.default_region(saved_name)?;
    region.label = saved_name.to_string();
    region.shapes = native_record_shapes(view, saved_name, record)?;
    if region.shapes.is_empty() {
        return Err(ImageError::InvalidMetadata(format!(
            "saved region '{saved_name}' does not contain any editable polygons"
        )));
    }
    Ok(region)
}

fn native_record_shapes(
    view: &OpenedImageView,
    saved_name: &str,
    record: &RecordValue,
) -> Result<Vec<ImageRegionShape>, ImageError> {
    match native_region_kind(saved_name, record)? {
        WCPOLYGON_NAME => Ok(vec![parse_wc_polygon_record(view, saved_name, record)?]),
        WCUNION_NAME => parse_wc_union_record(view, saved_name, record),
        other => Err(unsupported_saved_region_type_error(
            saved_name,
            other,
            "only WCPolygon and WCUnion-of-WCPolygon are editable in this wave",
        )),
    }
}

fn parse_wc_union_record(
    view: &OpenedImageView,
    saved_name: &str,
    record: &RecordValue,
) -> Result<Vec<ImageRegionShape>, ImageError> {
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

    let mut shapes = Vec::with_capacity(child_count);
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
        shapes.extend(native_record_shapes(view, saved_name, child)?);
    }
    Ok(shapes)
}

fn parse_wc_polygon_record(
    view: &OpenedImageView,
    saved_name: &str,
    record: &RecordValue,
) -> Result<ImageRegionShape, ImageError> {
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
            "saved region '{saved_name}' uses unsupported absrel mode {absrel}; only absolute WCPolygon coordinates are editable in this wave"
        )));
    }

    let pixel_axes = parse_i32_array_field(record, "pixelAxes", saved_name)?
        .into_iter()
        .map(|axis| if *one_rel { axis - 1 } else { axis })
        .collect::<Vec<_>>();
    let display_axes = view
        .axis_model()
        .display_axes
        .ok_or_else(|| ImageError::InvalidMetadata(view.status_line()))?;
    let expected_axes = vec![display_axes[0] as i32, display_axes[1] as i32];
    if pixel_axes != expected_axes {
        return Err(ImageError::InvalidMetadata(format!(
            "saved region '{saved_name}' targets pixel axes {:?}; active editable plane uses {:?}",
            pixel_axes, expected_axes
        )));
    }

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

    let axis_units = view.default_region(saved_name)?.axis_units;
    let x_values =
        convert_region_coordinate_values(&x_values, &x_unit, &axis_units[0], saved_name, "x")?;
    let y_values =
        convert_region_coordinate_values(&y_values, &y_unit, &axis_units[1], saved_name, "y")?;

    let shape = ImageRegionShape {
        vertices: x_values
            .into_iter()
            .zip(y_values)
            .map(|(x, y)| ImageRegionVertex { world: [x, y] })
            .collect(),
        closed: true,
    };
    validate_polygon_shape(&shape)?;
    Ok(shape)
}

fn wc_polygon_record(
    view: &OpenedImageView,
    region: &ImageRegion,
    shape: &ImageRegionShape,
) -> Result<RecordValue, ImageError> {
    validate_polygon_shape(shape)?;
    let mut record = base_wc_region_record(WCPOLYGON_NAME);
    record.upsert("oneRel", Value::Scalar(ScalarValue::Bool(true)));
    record.upsert(
        "pixelAxes",
        Value::Array(ArrayValue::from_i32_vec(vec![
            region.display_axes[0] as i32 + 1,
            region.display_axes[1] as i32 + 1,
        ])),
    );
    record.upsert(
        "x",
        Value::Record(quantum_vector_record(
            shape
                .vertices
                .iter()
                .map(|vertex| vertex.world[0])
                .collect(),
            &region.axis_units[0],
        )),
    );
    record.upsert(
        "y",
        Value::Record(quantum_vector_record(
            shape
                .vertices
                .iter()
                .map(|vertex| vertex.world[1])
                .collect(),
            &region.axis_units[1],
        )),
    );
    record.upsert(
        "absrel",
        Value::Scalar(ScalarValue::Int32(REGION_ABSREL_ABS)),
    );
    record.upsert(
        "coordinates",
        Value::Record(image_coordinates(&view.image).to_record()),
    );
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
        return Err(ImageError::InvalidMetadata(
            "saved region record is missing isRegion".into(),
        ));
    };
    let Some(Value::Scalar(ScalarValue::String(name))) = record.get("name") else {
        return Err(ImageError::InvalidMetadata(
            "saved region record is missing name".into(),
        ));
    };
    match *region_type {
        REGION_TYPE_WC => Ok(name.as_str()),
        1 => Err(ImageError::InvalidMetadata(format!(
            "saved region '{saved_name}' uses unsupported casacore type {name}; LC/pixel regions are not editable in this wave"
        ))),
        REGION_TYPE_ARRAY_SLICER => Err(ImageError::InvalidMetadata(format!(
            "saved region '{saved_name}' uses unsupported casacore type {name}; array-slicer regions are not editable in this wave"
        ))),
        other => Err(ImageError::InvalidMetadata(format!(
            "saved region uses unsupported casacore region type {other} ({name})"
        ))),
    }
}

fn unsupported_saved_region_type_error(
    saved_name: &str,
    class_name: &str,
    detail: &str,
) -> ImageError {
    ImageError::InvalidMetadata(format!(
        "saved region '{saved_name}' uses unsupported casacore type {class_name}; {detail}"
    ))
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
            "saved region '{saved_name}' targets unsupported editable unit '{target_unit}' for {field_name}: {error}"
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
    fn next_saved_region_name_skips_existing_indices() {
        let existing = vec![
            "Region 1".to_string(),
            "Region 2".to_string(),
            "Science".to_string(),
            "Region 4".to_string(),
        ];
        assert_eq!(next_saved_region_name(&existing), "Region 3");
    }

    #[test]
    fn next_region_mask_name_skips_existing_indices() {
        let existing = vec![
            "region_mask_1".to_string(),
            "mask".to_string(),
            "region_mask_3".to_string(),
        ];
        assert_eq!(next_region_mask_name(&existing), "region_mask_2");
    }

    #[test]
    fn rename_native_region_record_name_updates_parent_and_child_comments() {
        let mut child = RecordValue::default();
        child.upsert(
            "comment",
            Value::Scalar(ScalarValue::String("old".to_string())),
        );
        let mut regions = RecordValue::default();
        regions.upsert("*1", Value::Record(child));
        regions.upsert("nr", Value::Scalar(ScalarValue::Int32(1)));

        let mut record = RecordValue::default();
        record.upsert(
            "comment",
            Value::Scalar(ScalarValue::String("old".to_string())),
        );
        record.upsert("regions", Value::Record(regions));

        let renamed = rename_native_region_record_name(&record, "Science Region");
        let Some(Value::Scalar(ScalarValue::String(comment))) = renamed.get("comment") else {
            panic!("expected renamed parent comment");
        };
        assert_eq!(comment, "Science Region");
        let Value::Record(children) = renamed.get("regions").expect("regions present") else {
            panic!("expected regions subrecord");
        };
        let Value::Record(first_child) = children.get("*1").expect("child region present") else {
            panic!("expected child record");
        };
        let Some(Value::Scalar(ScalarValue::String(comment))) = first_child.get("comment") else {
            panic!("expected renamed child comment");
        };
        assert_eq!(comment, "Science Region");
    }

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
