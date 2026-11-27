// SPDX-License-Identifier: LGPL-3.0-or-later

//! Image shape and coordinate conformance checks (`ia.isconform`).

use std::path::Path;

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, DirectionCoordinate, RecordCoordinate,
};
use casa_core::{ArrayValue, RecordValue, ScalarValue, Value};
use casa_images::ImagePixelType;
use ndarray::Array2;

use super::attachment::AttachedImage;
use super::beams::subrecord;
use super::coords::validate_coordinate_indices;
use super::error::{ImageToolError, ImageToolResult, ToolLogLine};
use super::handle::ImageHandle;

const TOLERANCE: f64 = 1e-6;
const WCS_TOLERANCE: f64 = 1e-13;

/// Flags reporting which conformance criteria differ between an image and another image/csys.
#[derive(Debug, Clone, Default)]
pub struct ConformanceResult {
    /// `true` when all inspected properties conform.
    pub conforms: bool,
    /// Diagnostic messages explaining any non-conformance.
    pub logs: Vec<ToolLogLine>,
}

/// Checks whether `image` conforms with a target shape and/or [`CoordinateSystem`] (`isconform`).
pub fn check_conformance(
    image: &ImageHandle,
    other_shape: &[usize],
    other_coords: &CoordinateSystem,
    other_name: &str,
) -> ImageToolResult<ConformanceResult> {
    let shape = image.shape();
    let coords = image.coordinates();
    let mut logs = Vec::new();

    if shape != other_shape {
        logs.push(ToolLogLine::warning(format!(
            "Shapes do not match: {:?} vs {other_name} {:?}",
            shape, other_shape
        )));
        return Ok(ConformanceResult {
            conforms: false,
            logs,
        });
    }

    if coords.n_pixel_axes() != other_coords.n_pixel_axes()
        || coords.n_coordinates() != other_coords.n_coordinates()
    {
        logs.push(ToolLogLine::warning(format!(
            "Coordinate counts do not match {other_name}."
        )));
        return Ok(ConformanceResult {
            conforms: false,
            logs,
        });
    }

    for idx in 0..coords.n_coordinates() {
        let a = coords.coordinate(idx);
        let b = other_coords.coordinate(idx);
        if a.coordinate_type() != b.coordinate_type() {
            logs.push(ToolLogLine::warning(format!(
                "Coordinate types differ at index {idx}: {:?} vs {:?}.",
                a.coordinate_type(),
                b.coordinate_type()
            )));
            return Ok(ConformanceResult {
                conforms: false,
                logs,
            });
        }
        if let (CoordinateModel::Direction(da), CoordinateModel::Direction(db)) = (a, b)
            && da.direction_ref() != db.direction_ref()
        {
            logs.push(ToolLogLine::warning(format!(
                "Direction reference frames differ: {:?} vs {:?}.",
                da.direction_ref(),
                db.direction_ref()
            )));
            return Ok(ConformanceResult {
                conforms: false,
                logs,
            });
        }
        if let (CoordinateModel::Stokes(sa), CoordinateModel::Stokes(sb)) = (a, b)
            && sa.stokes() != sb.stokes()
        {
            logs.push(ToolLogLine::warning("Stokes axes do not match."));
            return Ok(ConformanceResult {
                conforms: false,
                logs,
            });
        }
        let pix_a = coords.coordinate_pixel_axes(idx);
        let pix_b = other_coords.coordinate_pixel_axes(idx);
        if pix_a != pix_b {
            logs.push(ToolLogLine::warning(format!(
                "Coordinate axis mapping differs at index {idx}."
            )));
            return Ok(ConformanceResult {
                conforms: false,
                logs,
            });
        }
    }

    Ok(ConformanceResult {
        conforms: true,
        logs,
    })
}

/// Verifies `conforms`, raising an [`ImageToolError`] when non-conformant.
pub fn require_conformance(
    image: &ImageHandle,
    other_shape: &[usize],
    other_coords: &CoordinateSystem,
    other_name: &str,
) -> ImageToolResult<Vec<ToolLogLine>> {
    let res = check_conformance(image, other_shape, other_coords, other_name)?;
    if !res.conforms {
        let reason = res
            .logs
            .first()
            .map_or("Images do not conform.", |l| l.message.as_str());
        return Err(ImageToolError::new(reason));
    }
    Ok(res.logs)
}

fn near(left: f64, right: f64, tolerance: f64) -> bool {
    if left == right {
        return true;
    }
    if left == 0.0 || right == 0.0 {
        return left.abs().max(right.abs()) <= (1.0 + tolerance) * f64::MIN_POSITIVE;
    }
    (left > 0.0) == (right > 0.0) && (left - right).abs() <= tolerance * left.abs().max(right.abs())
}

fn near_values(left: &[f64], right: &[f64], tolerance: f64) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(&left, &right)| near(left, right, tolerance))
}

fn scalar(value: Option<&Value>) -> Option<f64> {
    match value {
        Some(Value::Scalar(ScalarValue::Float64(value))) => Some(*value),
        Some(Value::Scalar(ScalarValue::Float32(value))) => Some(f64::from(*value)),
        Some(Value::Scalar(ScalarValue::Int32(value))) => Some(f64::from(*value)),
        Some(Value::Scalar(ScalarValue::Int64(value))) => Some(*value as f64),
        Some(Value::Array(ArrayValue::Float64(value))) if value.shape() == [1, 1] => {
            value.first().copied()
        }
        _ => None,
    }
}

fn near_matrix(left: Option<&Value>, right: Option<&Value>) -> bool {
    if let (Some(left), Some(right)) = (scalar(left), scalar(right)) {
        return near(left, right, TOLERANCE);
    }
    match (left, right) {
        (
            Some(Value::Array(ArrayValue::Float64(left))),
            Some(Value::Array(ArrayValue::Float64(right))),
        ) => {
            left.shape() == right.shape()
                && left
                    .iter()
                    .zip(right)
                    .all(|(&left, &right)| near(left, right, TOLERANCE))
        }
        _ => left == right,
    }
}

fn velocity_field<'a>(
    stored: &'a RecordValue,
    defaults: &'a RecordValue,
    name: &str,
    legacy: &str,
) -> Option<&'a Value> {
    stored
        .get(name)
        .or_else(|| stored.get(legacy))
        .or_else(|| defaults.get(name))
}

fn pc_matrix(record: &RecordValue) -> Option<&Value> {
    subrecord(record, "wcs")
        .or_else(|| subrecord(record, "tabular"))
        .unwrap_or(record)
        .get("pc")
}

fn linear_labels_near(left: &[String], right: &[String]) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            left.split_whitespace()
                .next()
                .unwrap_or("")
                .eq_ignore_ascii_case(right.split_whitespace().next().unwrap_or(""))
        })
}

fn normalized_direction_transform(coordinate: &DirectionCoordinate) -> (Vec<f64>, Array2<f64>) {
    let mut increments = coordinate.increment();
    let mut matrix = coordinate.pc_matrix().clone();
    for (index, mut row) in matrix.rows_mut().into_iter().enumerate() {
        let power = row.iter().map(|value| value * value).sum::<f64>();
        if power != 0.0 && power != 1.0 {
            let diagonal = row[index];
            let factor = power.sqrt() * (diagonal / diagonal.abs());
            row.mapv_inplace(|value| value / factor);
            increments[index] *= factor;
        }
    }
    (increments, matrix)
}

fn directions_near(left: &DirectionCoordinate, right: &DirectionCoordinate) -> bool {
    let (left_inc, left_pc) = normalized_direction_transform(left);
    let (right_inc, right_pc) = normalized_direction_transform(right);
    left.axis_names() == right.axis_names()
        && left.world_units() == right.world_units()
        && left.direction_ref() == right.direction_ref()
        && left.conversion_direction_ref() == right.conversion_direction_ref()
        && left.projection().projection_type() == right.projection().projection_type()
        && near_values(
            left.projection().parameters(),
            right.projection().parameters(),
            TOLERANCE,
        )
        && near(left.longpole(), right.longpole(), WCS_TOLERANCE)
        && near(left.native_pole()[1], right.native_pole()[1], WCS_TOLERANCE)
        && near_values(&left.reference_value(), &right.reference_value(), TOLERANCE)
        && near_values(&left.reference_pixel(), &right.reference_pixel(), TOLERANCE)
        && near_values(&left_inc, &right_inc, TOLERANCE)
        && left_pc
            .iter()
            .zip(&right_pc)
            .all(|(&left, &right)| near(left, right, TOLERANCE))
}

fn coordinate_near(
    left: &CoordinateModel,
    left_record: &RecordValue,
    right: &CoordinateModel,
    right_record: &RecordValue,
) -> bool {
    if let (CoordinateModel::Direction(left), CoordinateModel::Direction(right)) = (left, right) {
        return directions_near(left, right);
    }
    let descriptors_near = if matches!(left, CoordinateModel::Linear(_)) {
        linear_labels_near(&left.axis_names(), &right.axis_names())
            && linear_labels_near(&left.axis_units(), &right.axis_units())
    } else {
        left.axis_names() == right.axis_names() && left.axis_units() == right.axis_units()
    };
    if !descriptors_near {
        return false;
    }
    let left_canonical = left.to_record();
    let right_canonical = right.to_record();
    // Casacore compares the number of Stokes values without comparing their labels.
    if let (CoordinateModel::Stokes(left), CoordinateModel::Stokes(right)) = (left, right) {
        return left.stokes().len() == right.stokes().len();
    }
    let reference_tolerance = if matches!(left, CoordinateModel::Spectral(_)) {
        WCS_TOLERANCE
    } else {
        TOLERANCE
    };
    if !near_values(
        &left.reference_value(),
        &right.reference_value(),
        reference_tolerance,
    ) {
        return false;
    }
    let single_tabular_channel = matches!(left, CoordinateModel::Spectral(_))
        && matches!(
            subrecord(left_record, "tabular").and_then(|table| table.get("pixelvalues")),
            Some(Value::Array(ArrayValue::Float64(values))) if values.len() == 1
        );
    if !single_tabular_channel
        && (!near_values(&left.reference_pixel(), &right.reference_pixel(), TOLERANCE)
            || !near_values(&left.increment(), &right.increment(), TOLERANCE)
            || !near_matrix(
                pc_matrix(left_record).or_else(|| pc_matrix(&left_canonical)),
                pc_matrix(right_record).or_else(|| pc_matrix(&right_canonical)),
            ))
    {
        return false;
    }
    match (left, right) {
        (CoordinateModel::Spectral(left), CoordinateModel::Spectral(right)) => {
            left.frequency_ref() == right.frequency_ref()
                && left.has_undefined_frame() == right.has_undefined_frame()
                && left.world_frequency_ref() == right.world_frequency_ref()
                && near(left.rest_frequency(), right.rest_frequency(), TOLERANCE)
                && near_values(left.rest_frequencies(), right.rest_frequencies(), TOLERANCE)
                && scalar(velocity_field(
                    left_record,
                    &left_canonical,
                    "velType",
                    "prefVelType",
                )) == scalar(velocity_field(
                    right_record,
                    &right_canonical,
                    "velType",
                    "prefVelType",
                ))
                && velocity_field(left_record, &left_canonical, "velUnit", "prefVelUnit")
                    == velocity_field(right_record, &right_canonical, "velUnit", "prefVelUnit")
        }
        (CoordinateModel::Tabular(left), CoordinateModel::Tabular(right)) => {
            near_values(left.pixel_values(), right.pixel_values(), TOLERANCE)
                && near_values(left.world_values(), right.world_values(), TOLERANCE)
        }
        (CoordinateModel::Linear(_), CoordinateModel::Linear(_)) => true,
        _ => false,
    }
}

struct PlacedAxis {
    coordinate: RecordCoordinate,
    model: CoordinateModel,
    world_axes: Vec<i64>,
}

fn placements(record: &RecordValue) -> ImageToolResult<Vec<PlacedAxis>> {
    let declared = validate_coordinate_indices(record)?;
    let mut layout = CoordinateSystem::record_coordinates(record)?
        .into_iter()
        .map(|coordinate| {
            let number = coordinate
                .field
                .trim_start_matches(|character: char| !character.is_ascii_digit())
                .parse::<usize>()
                .expect("numbered coordinate field");
            (number, coordinate)
        })
        .collect::<Vec<_>>();
    if layout.len() != declared {
        return Err(ImageToolError::new(
            "Coordinate conformance for this record is unfinished (#1002)",
        ));
    }
    layout.sort_unstable_by_key(|(number, _)| *number);
    let mut next_world = 0;
    layout
        .into_iter()
        .map(|(number, coordinate)| {
            let model = coordinate.model(record)?;
            let axes = model.n_world_axes();
            let world_axes: Vec<_> = match record.get(&format!("worldmap{number}")) {
                Some(Value::Array(ArrayValue::Int32(values))) => {
                    values.iter().map(|&value| i64::from(value)).collect()
                }
                Some(Value::Array(ArrayValue::Int64(values))) => values.iter().copied().collect(),
                None => (next_world..next_world + axes as i64).collect(),
                _ => {
                    return Err(ImageToolError::new(
                        "Invalid image coordinate world-axis map",
                    ));
                }
            };
            if world_axes.len() != axes {
                return Err(ImageToolError::new(
                    "Invalid image coordinate world-axis map",
                ));
            }
            next_world += axes as i64;
            Ok(PlacedAxis {
                coordinate,
                model,
                world_axes,
            })
        })
        .collect()
}

/// Compares two casacore coordinate records within CASA `ImageMetaData` tolerances.
pub fn coordinates_conform(
    left: &RecordValue,
    right: &RecordValue,
    exact_axis_names: bool,
) -> ImageToolResult<bool> {
    let left_system = CoordinateSystem::from_record(left)?;
    let right_system = CoordinateSystem::from_record(right)?;
    if left_system.n_coordinates() != right_system.n_coordinates()
        || left_system.n_pixel_axes() != right_system.n_pixel_axes()
        || left_system.n_world_axes() != right_system.n_world_axes()
    {
        return Ok(false);
    }
    let left_layout = placements(left)?;
    let right_layout = placements(right)?;
    for (lhs, rhs) in left_layout.iter().zip(&right_layout) {
        if lhs.model.coordinate_type() != rhs.model.coordinate_type()
            || lhs.coordinate.pixel_axes != rhs.coordinate.pixel_axes
            || lhs.world_axes != rhs.world_axes
        {
            return Ok(false);
        }
        if exact_axis_names {
            let left_names = lhs.model.axis_names();
            let right_names = rhs.model.axis_names();
            if lhs
                .world_axes
                .iter()
                .enumerate()
                .any(|(index, &axis)| axis >= 0 && left_names.get(index) != right_names.get(index))
            {
                return Ok(false);
            }
        }
        if lhs.world_axes.iter().any(|&axis| axis >= 0) {
            let left_record = subrecord(left, &lhs.coordinate.field).expect("parsed coordinate");
            let right_record = subrecord(right, &rhs.coordinate.field).expect("parsed coordinate");
            if !coordinate_near(&lhs.model, left_record, &rhs.model, right_record) {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// Checks whether `attached` matches `other_path` in shape and coordinate system (`ia.isconform`).
pub fn is_conform(
    attached: &AttachedImage,
    other_raw: &str,
    other_path: &Path,
) -> ImageToolResult<bool> {
    if attached.lock().kind() != ImagePixelType::Float32 {
        return Err(ImageToolError::new(
            "Exception: This method only supports Float valued images.",
        ));
    }
    if other_raw.is_empty() {
        return Err(ImageToolError::new("Exception: File name is empty."));
    }
    if !other_path.exists() {
        return Err(ImageToolError::new(format!(
            "Exception: File {other_raw} does not exist.."
        )));
    }
    let other = AttachedImage::open(other_path).map_err(|_| {
        ImageToolError::new(format!("Exception: Unable to open image {other_raw}."))
    })?;
    let (shape, coordinates) = {
        let image = attached.lock();
        (image.shape().to_vec(), image.coordinate_record())
    };
    let other = other.lock();
    if !matches!(
        other.kind(),
        ImagePixelType::Float32 | ImagePixelType::Complex32
    ) {
        return Err(ImageToolError::new(
            "Exception: Unsupported image pixel data type.",
        ));
    }
    if shape != other.shape() {
        return Ok(false);
    }
    coordinates_conform(&coordinates, &other.coordinate_record(), true)
}
