// SPDX-License-Identifier: LGPL-3.0-or-later
//! One-axis tabular coordinate with lookup table interpolation.
//!
//! [`TabularCoordinate`] maps between pixel and world values using a pair of
//! lookup tables. Intermediate values are found by linear interpolation.
//! This is useful for non-linear axes (e.g. irregularly-spaced frequency
//! channels or a velocity axis with non-uniform spacing).
//!
//! Corresponds to C++ `TabularCoordinate`.

use casa_core::{ArrayValue, RecordValue, Value};

use crate::coordinate::{Coordinate, CoordinateType};
use crate::error::CoordinateError;
use crate::record_utils::{get_optional_string, get_optional_vec_string, get_required_vec_f64};

const CASACORE_PIXEL_TOLERANCE: f64 = 1e-13;

/// An edit to one of a tabular coordinate's live reference parameters.
#[derive(Clone, Copy)]
pub enum TabularEdit {
    /// Move the live reference pixel.
    ReferencePixel(f64),
    /// Change the live reference world value.
    ReferenceValue(f64),
    /// Change the live world-axis increment.
    Increment(f64),
}

impl TabularEdit {
    pub(crate) fn apply(self, [mut crval, mut cdelt, mut crpix]: [f64; 3]) -> [f64; 3] {
        match self {
            Self::ReferencePixel(value) => crpix = value,
            Self::ReferenceValue(value) => crval = value,
            Self::Increment(value) => cdelt = value,
        }
        [crval, cdelt, crpix]
    }
}

pub(crate) fn scaled_world(value: f64, reference: f64, transform: f64) -> f64 {
    if transform == 1.0 {
        value
    } else {
        reference + transform * (value - reference)
    }
}

pub(crate) fn validate_transform_samples(
    values: &[f64],
    reference: f64,
    transform: f64,
) -> Result<(), CoordinateError> {
    let invalid = || {
        CoordinateError::InvalidRecord(
            "linear transform would produce invalid tabular samples".into(),
        )
    };
    if !transform.is_finite() || transform == 0.0 || values.len() < 2 {
        return Err(invalid());
    }
    let overall_order = values[values.len() - 1].partial_cmp(&values[0]);
    if !matches!(
        overall_order,
        Some(std::cmp::Ordering::Less | std::cmp::Ordering::Greater)
    ) {
        return Err(invalid());
    }
    let mut previous = scaled_world(values[0], reference, transform);
    for (interval, pair) in values.windows(2).enumerate() {
        let next = scaled_world(pair[1], reference, transform);
        let order = if interval == 0 {
            pair[1].partial_cmp(&pair[0])
        } else {
            overall_order
        };
        let expected = order.map(|order| {
            if transform < 0.0 {
                order.reverse()
            } else {
                order
            }
        });
        if !previous.is_finite() || !next.is_finite() || next.partial_cmp(&previous) != expected {
            return Err(invalid());
        }
        previous = next;
    }
    Ok(())
}

pub(crate) fn sample_reference(record: &RecordValue) -> Result<[f64; 3], CoordinateError> {
    let read = |field| {
        let values = get_required_vec_f64(record, field)?;
        match values.as_slice() {
            [value] if value.is_finite() => Ok(*value),
            _ => Err(CoordinateError::InvalidRecord(format!(
                "invalid tabular {field}"
            ))),
        }
    };
    Ok([read("crval")?, read("cdelt")?, read("crpix")?])
}

pub(crate) fn validate_live_edit(edit: TabularEdit) -> Result<(), CoordinateError> {
    let value = match edit {
        TabularEdit::ReferencePixel(value)
        | TabularEdit::ReferenceValue(value)
        | TabularEdit::Increment(value) => value,
    };
    if !value.is_finite() || (value == 0.0 && matches!(edit, TabularEdit::Increment(_))) {
        return Err(CoordinateError::InvalidRecord(
            "invalid tabular reference parameter".into(),
        ));
    }
    Ok(())
}

pub(crate) struct EditedTable {
    pub(crate) old: f64,
    pub(crate) values: Vec<f64>,
    pub(crate) increment: f64,
}

/// Apply casacore's tabular affine edit to its sampled world values.
pub(crate) fn edit_table(
    pixel_values: &[f64],
    world_values: &[f64],
    reference_value: f64,
    increment: f64,
    reference_pixel: f64,
    edit: TabularEdit,
) -> Result<EditedTable, CoordinateError> {
    if pixel_values.len() < 2 || pixel_values.len() != world_values.len() || !increment.is_normal()
    {
        return Err(CoordinateError::InvalidRecord(
            "invalid tabular coordinate or subnormal increment (#977)".into(),
        ));
    }
    let (old, values) = match edit {
        TabularEdit::ReferencePixel(value) => (
            reference_pixel,
            world_values
                .iter()
                .map(|world| *world - increment * (value - reference_pixel))
                .collect::<Vec<_>>(),
        ),
        TabularEdit::ReferenceValue(value) => (
            reference_value,
            world_values
                .iter()
                .map(|world| *world + (value - reference_value))
                .collect::<Vec<_>>(),
        ),
        TabularEdit::Increment(value) => (
            increment,
            world_values
                .iter()
                .map(|world| reference_value + (*world - reference_value) * (value / increment))
                .collect::<Vec<_>>(),
        ),
    };
    if values.iter().any(|value| !value.is_finite()) {
        return Err(CoordinateError::InvalidRecord(
            "tabular edit produces non-finite values (#977)".into(),
        ));
    }
    let first = values[0];
    let last = values[values.len() - 1];
    let span = pixel_values[pixel_values.len() - 1] - pixel_values[0];
    let increment = (last - first) / span;
    let increasing = last > first;
    let reversed = increasing != (world_values[world_values.len() - 1] > world_values[0]);
    let first_order = world_values[1]
        .partial_cmp(&world_values[0])
        .map(|order| if reversed { order.reverse() } else { order });
    if !increment.is_normal()
        || values[1].partial_cmp(&first) != first_order
        || values
            .windows(2)
            .skip(1)
            .any(|pair| pair[0] == pair[1] || (pair[1] > pair[0]) != increasing)
    {
        return Err(CoordinateError::InvalidRecord(
            "tabular edit produces an invalid increment (#977)".into(),
        ));
    }
    Ok(EditedTable {
        old,
        values,
        increment,
    })
}

/// A one-axis coordinate defined by a pair of lookup tables.
///
/// The `pixel_values` and `world_values` vectors have the same length.
/// Each table can increase or decrease independently. A single sample
/// defines a constant mapping in both directions.
///
/// Corresponds to C++ `TabularCoordinate`.
#[derive(Debug, Clone)]
pub struct TabularCoordinate {
    pixel_values: Vec<f64>,
    world_values: Vec<f64>,
    /// Linear tabular coordinates carry no sampled lookup table.
    linear: Option<(f64, f64, f64)>,
    sample_reference: Option<[f64; 3]>,
    linear_transform: f64,
    name: String,
    unit: String,
}

impl TabularCoordinate {
    /// Creates a new tabular coordinate.
    ///
    /// Both vectors must have the same length (at least one). The pixel values
    /// should be monotonically increasing or decreasing.
    ///
    /// # Panics
    ///
    /// Panics if the vectors have different lengths or are empty.
    pub fn new(
        pixel_values: Vec<f64>,
        world_values: Vec<f64>,
        name: impl Into<String>,
        unit: impl Into<String>,
    ) -> Self {
        assert_eq!(
            pixel_values.len(),
            world_values.len(),
            "pixel and world tables must have the same length"
        );
        assert!(
            !pixel_values.is_empty(),
            "tabular coordinate requires at least one entry"
        );
        Self {
            pixel_values,
            world_values,
            linear: None,
            sample_reference: None,
            linear_transform: 1.0,
            name: name.into(),
            unit: unit.into(),
        }
    }

    /// Create a tabular coordinate from user-supplied samples.
    ///
    /// # Errors
    /// Returns an error for empty, unmatched, nonfinite, or unusable tables.
    pub fn from_samples(
        pixel: Vec<f64>,
        world: Vec<f64>,
        name: impl Into<String>,
        unit: impl Into<String>,
    ) -> Result<Self, CoordinateError> {
        Self::validate_samples(&pixel, &world)?;
        Ok(Self::new(pixel, world, name, unit))
    }

    pub(crate) fn validate_samples(pixel: &[f64], world: &[f64]) -> Result<(), CoordinateError> {
        let invalid = |message: &str| CoordinateError::InvalidRecord(message.into());
        if pixel.is_empty() || pixel.len() != world.len() {
            return Err(invalid(
                "tabular pixel and world tables must have the same nonzero length",
            ));
        }
        if pixel.iter().chain(world).any(|x| !x.is_finite()) {
            return Err(invalid("tabular samples must be finite"));
        }
        if pixel.len() > 1 {
            let last = pixel.len() - 1;
            let dp = pixel[last] - pixel[0];
            let dw = world[last] - world[0];
            if dp == 0.0 {
                return Err(invalid(
                    "TabularCoordinate::TabularCoordinate - illegal table first and last pixel values are the same",
                ));
            }
            if dw / dp == 0.0 {
                return Err(invalid(
                    "TabularCoordinate - start and end values in table must differ",
                ));
            }
            if !dp.is_finite() || !dw.is_finite() || !(dw / dp).is_finite() {
                return Err(invalid("tabular sample span must be finite"));
            }
            // Interpolate1D compares adjacent pixels with nearAbs before it
            // can evaluate or serialize the world samples.
            if pixel
                .windows(2)
                .any(|pair| (pair[1] - pair[0]).abs() <= CASACORE_PIXEL_TOLERANCE)
            {
                return Err(invalid("tabular table has repeated pixel values"));
            }
            // Casacore allows the first interval to run in either direction.
            if pixel
                .windows(2)
                .skip(1)
                .any(|pair| (pair[1] - pair[0]) * dp.signum() <= 0.0)
                || world
                    .windows(2)
                    .skip(1)
                    .any(|pair| (pair[1] - pair[0]) * dw.signum() <= 0.0)
            {
                return Err(invalid(
                    "TabularCoordinate - pixel and world values must increase or decrease monotonically",
                ));
            }
        }
        Ok(())
    }

    /// Creates a linear tabular axis with no samples in its lookup table.
    pub fn linear(
        reference_value: f64,
        increment: f64,
        reference_pixel: f64,
        name: impl Into<String>,
        unit: impl Into<String>,
    ) -> Self {
        Self {
            pixel_values: Vec::new(),
            world_values: Vec::new(),
            linear: Some((reference_value, increment, reference_pixel)),
            sample_reference: None,
            linear_transform: 1.0,
            name: name.into(),
            unit: unit.into(),
        }
    }

    /// Returns the pixel lookup table.
    pub fn pixel_values(&self) -> &[f64] {
        &self.pixel_values
    }

    /// Returns the world lookup table.
    pub fn world_values(&self) -> &[f64] {
        &self.world_values
    }

    fn has_reversed_pixel_interval(&self) -> bool {
        self.pixel_values.len() > 2
            && (self.pixel_values[1] > self.pixel_values[0])
                != (self.pixel_values.last().unwrap() > &self.pixel_values[0])
    }

    /// The multiplier applied to offsets from the reference world value.
    pub fn linear_transform(&self) -> f64 {
        self.linear_transform
    }

    /// Set the live linear transform of this coordinate.
    ///
    /// # Errors
    /// Returns an error for a nonfinite transform or collapsed lookup table.
    pub fn with_linear_transform(mut self, transform: f64) -> Result<Self, CoordinateError> {
        if self.world_values.len() > 1 {
            validate_transform_samples(&self.world_values, self.reference_world(), transform)?;
        } else if !transform.is_finite() {
            return Err(CoordinateError::InvalidRecord(
                "linear transform would produce invalid tabular samples".into(),
            ));
        }
        self.linear_transform = transform;
        if self.pixel_values.len() == 1 && !self.constant_world().is_finite() {
            return Err(CoordinateError::InvalidRecord(
                "linear transform would produce invalid tabular samples".into(),
            ));
        }
        Ok(self)
    }

    fn scaled_world(&self, value: f64) -> f64 {
        scaled_world(value, self.reference_world(), self.linear_transform)
    }

    fn reference_world(&self) -> f64 {
        if let Some((value, _, _)) = self.linear {
            value
        } else if let Some([value, _, _]) = self.sample_reference {
            value
        } else {
            self.world_values[0]
        }
    }

    fn constant_world(&self) -> f64 {
        match self.sample_reference {
            Some([value, increment, pixel]) => {
                value + increment * self.linear_transform * (self.pixel_values[0] - pixel)
            }
            None => self.world_values[0],
        }
    }

    /// Restore live reference parameters from an in-memory record containing
    /// unscaled lookup values. Public record restoration normalizes them.
    ///
    /// # Errors
    /// Returns an error if a required reference parameter is missing.
    pub fn with_live_reference(mut self, record: &RecordValue) -> Result<Self, CoordinateError> {
        let [crval, cdelt, crpix] = sample_reference(record)?;
        if self.linear.is_some() {
            self.linear = Some((crval, cdelt, crpix));
        } else {
            if self.has_reversed_pixel_interval() {
                // The stored first sample was evaluated in a later interval.
                // Recover the initial endpoint of the live interpolation.
                self.world_values[0] = crval + cdelt * (self.pixel_values[0] - crpix);
            }
            self.sample_reference = Some([crval, cdelt, crpix]);
        }
        Ok(self)
    }

    /// Edit the live reference without renormalizing a sampled coordinate.
    ///
    /// # Errors
    /// Returns an error if the edit produces an invalid mapping.
    pub fn edit_live_reference(&mut self, edit: TabularEdit) -> Result<(), CoordinateError> {
        if self.pixel_values.len() == 1 {
            self.edit(edit)?;
            return Ok(());
        }
        validate_live_edit(edit)?;
        let reference = [
            self.reference_value()[0],
            self.increment()[0],
            self.reference_pixel()[0],
        ];
        self.edit(edit)?;
        if self.linear.is_none() {
            self.sample_reference = Some(edit.apply(reference));
        }
        Ok(())
    }

    pub(crate) fn edit(&mut self, edit: TabularEdit) -> Result<f64, CoordinateError> {
        if let Some((ref mut crval, ref mut cdelt, ref mut crpix)) = self.linear {
            return Ok(match edit {
                TabularEdit::ReferencePixel(value) => std::mem::replace(crpix, value),
                TabularEdit::ReferenceValue(value) => std::mem::replace(crval, value),
                TabularEdit::Increment(value) => std::mem::replace(cdelt, value),
            });
        }
        if self.pixel_values.len() == 1 {
            let [mut world, mut increment, mut pixel] =
                self.sample_reference
                    .unwrap_or([self.world_values[0], 0.0, self.pixel_values[0]]);
            let (slot, value) = match edit {
                TabularEdit::ReferencePixel(value) => (&mut pixel, value),
                TabularEdit::ReferenceValue(value) => (&mut world, value),
                TabularEdit::Increment(value) => (&mut increment, value),
            };
            let old = std::mem::replace(slot, value);
            if !value.is_finite()
                || !(world + increment * self.linear_transform * (self.pixel_values[0] - pixel))
                    .is_finite()
            {
                return Err(CoordinateError::InvalidRecord(
                    "invalid tabular reference parameter".into(),
                ));
            }
            self.sample_reference = Some([world, increment, pixel]);
            return Ok(old);
        }
        let updated = edit_table(
            &self.pixel_values,
            &self.world_values,
            self.reference_value()[0],
            self.increment()[0],
            self.reference_pixel()[0],
            edit,
        )?;
        self.world_values = updated.values;
        Ok(updated.old)
    }

    /// Reconstructs a tabular coordinate from a serialized record.
    pub fn from_record(rec: &RecordValue) -> Result<Self, CoordinateError> {
        let pixel_values = get_required_vec_f64(rec, "pixelvalues")?;
        let world_values = get_required_vec_f64(rec, "worldvalues")?;
        let name = get_optional_string(rec, "name")
            .or_else(|| get_optional_vec_string(rec, "axes").and_then(|v| v.into_iter().next()))
            .unwrap_or_else(|| "Tabular".into());
        let unit = get_optional_string(rec, "unit")
            .or_else(|| get_optional_vec_string(rec, "units").and_then(|v| v.into_iter().next()))
            .unwrap_or_default();
        if pixel_values.is_empty() && world_values.is_empty() {
            let read = |field| {
                let values = get_required_vec_f64(rec, field)?;
                if values.len() == 1 {
                    Ok(values[0])
                } else {
                    Err(CoordinateError::InvalidRecord(format!(
                        "invalid tabular {field}"
                    )))
                }
            };
            return Ok(Self::linear(
                read("crval")?,
                read("cdelt")?,
                read("crpix")?,
                name,
                unit,
            ));
        }
        if pixel_values.len() != world_values.len() {
            return Err(CoordinateError::InvalidRecord(
                "tabular coordinate requires matching pixel/world tables".into(),
            ));
        }
        Ok(Self::new(pixel_values, world_values, name, unit))
    }
}

/// Linear interpolation: given tables `xs` and `ys`, find the `y` value
/// corresponding to `x` by interpolating (or extrapolating) between the
/// nearest bracketing entries.
fn interpolate(
    xs: impl Fn(usize) -> f64,
    ys: &[f64],
    x: f64,
    tolerance: f64,
) -> Result<f64, CoordinateError> {
    let n = ys.len();
    if n < 2 || x.is_nan() {
        return Err(CoordinateError::ConversionFailed(
            "tabular interpolation requires a valid argument and at least 2 entries".into(),
        ));
    }

    // Determine if xs is increasing or decreasing
    let increasing = xs(n - 1) > xs(0);

    // Search in sample order with the lower middle position, as casacore
    // does. The first interval can run opposite to the others.
    let mut lo = 0;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo - 1) / 2;
        let sample = xs(mid);
        if (increasing && sample < x) || (!increasing && sample > x) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    // Clamp to valid interval for interpolation/extrapolation
    let lo = lo.saturating_sub(1).min(n - 2);
    let hi = lo + 1;

    let lower = xs(lo);
    let upper = xs(hi);
    let dx = upper - lower;
    if !dx.is_finite() || dx.abs() <= tolerance {
        return Err(CoordinateError::ConversionFailed(
            "Interpolate1D::operator() data has repeated x values".into(),
        ));
    }
    if x == lower {
        return Ok(ys[lo]);
    }
    if x == upper {
        return Ok(ys[hi]);
    }
    let t = (x - lower) / dx;
    Ok(ys[lo] + t * (ys[hi] - ys[lo]))
}

pub(crate) fn interpolate_samples(xs: &[f64], ys: &[f64], x: f64) -> Result<f64, CoordinateError> {
    if xs.is_empty() || xs.len() != ys.len() {
        return Err(CoordinateError::ConversionFailed(
            "tabular interpolation requires matching lookup tables with at least one entry".into(),
        ));
    }
    if xs.len() == 1 {
        return Ok(ys[0]);
    }
    interpolate(|index| xs[index], ys, x, 0.0)
}

pub(crate) fn inverse_sampled_axis(
    pixels: &[f64],
    worlds: &[f64],
    reference: f64,
    transform: f64,
    world: f64,
) -> Result<f64, CoordinateError> {
    if pixels.len() < 2 || pixels.len() != worlds.len() {
        return interpolate_samples(worlds, pixels, world);
    }
    // Casacore's inverse channel corrector interpolates in the average
    // pixel domain, so its duplicate tolerance is independent of world units.
    let last = pixels.len() - 1;
    let origin = worlds[0];
    let increment = (worlds[last] - origin) / (pixels[last] - pixels[0]);
    let corrected_pixel = |value| (value - origin) / increment + pixels[0];
    let target = corrected_pixel(reference) + (world - reference) / (increment * transform);
    interpolate(
        |index| corrected_pixel(worlds[index]),
        pixels,
        target,
        CASACORE_PIXEL_TOLERANCE,
    )
}

impl Coordinate for TabularCoordinate {
    fn coordinate_type(&self) -> CoordinateType {
        CoordinateType::Tabular
    }

    fn n_pixel_axes(&self) -> usize {
        1
    }

    fn n_world_axes(&self) -> usize {
        1
    }

    fn to_world(&self, pixel: &[f64]) -> Result<Vec<f64>, CoordinateError> {
        if pixel.len() != 1 {
            return Err(CoordinateError::DimensionMismatch {
                expected: 1,
                got: pixel.len(),
            });
        }
        let world = if let Some((crval, cdelt, crpix)) = self.linear {
            crval + cdelt * self.linear_transform * (pixel[0] - crpix)
        } else if self.pixel_values.len() == 1 {
            self.constant_world()
        } else {
            self.scaled_world(interpolate_samples(
                &self.pixel_values,
                &self.world_values,
                pixel[0],
            )?)
        };
        Ok(vec![world])
    }

    fn to_pixel(&self, world: &[f64]) -> Result<Vec<f64>, CoordinateError> {
        if world.len() != 1 {
            return Err(CoordinateError::DimensionMismatch {
                expected: 1,
                got: world.len(),
            });
        }
        let pixel = if let Some((crval, cdelt, crpix)) = self.linear {
            if cdelt.abs() < 1e-300 {
                return Err(CoordinateError::ConversionFailed(
                    "zero tabular increment".into(),
                ));
            }
            crpix + (world[0] - crval) / (cdelt * self.linear_transform)
        } else {
            inverse_sampled_axis(
                &self.pixel_values,
                &self.world_values,
                self.reference_world(),
                self.linear_transform,
                world[0],
            )?
        };
        Ok(vec![pixel])
    }

    fn reference_value(&self) -> Vec<f64> {
        vec![self.reference_world()]
    }

    fn reference_pixel(&self) -> Vec<f64> {
        let value = if let Some((_, _, value)) = self.linear {
            value
        } else if let Some([_, _, value]) = self.sample_reference {
            value
        } else {
            self.pixel_values[0]
        };
        vec![value]
    }

    fn increment(&self) -> Vec<f64> {
        if let Some((_, cdelt, _)) = self.linear {
            return vec![cdelt];
        }
        if let Some([_, cdelt, _]) = self.sample_reference {
            return vec![cdelt];
        }
        // Casacore derives the global increment from the table endpoints.
        let last = self.pixel_values.len() - 1;
        let dp = self.pixel_values[last] - self.pixel_values[0];
        let dw = self.world_values[last] - self.world_values[0];
        if dp.abs() < 1e-300 {
            vec![0.0]
        } else {
            vec![dw / dp]
        }
    }

    fn axis_names(&self) -> Vec<String> {
        vec![self.name.clone()]
    }

    fn axis_units(&self) -> Vec<String> {
        vec![self.unit.clone()]
    }
}

impl TabularCoordinate {
    pub(crate) fn to_record(&self) -> RecordValue {
        let mut rec = RecordValue::default();
        rec.upsert(
            "crval",
            Value::Array(ArrayValue::from_f64_vec(self.reference_value())),
        );
        rec.upsert(
            "crpix",
            Value::Array(ArrayValue::from_f64_vec(self.reference_pixel())),
        );
        rec.upsert(
            "cdelt",
            Value::Array(ArrayValue::from_f64_vec(self.increment())),
        );
        rec.upsert(
            "pc",
            Value::Array(ArrayValue::Float64(
                ndarray::ArrayD::from_shape_vec(
                    ndarray::IxDyn(&[1, 1]),
                    vec![self.linear_transform],
                )
                .expect("1x1 tabular pc matrix"),
            )),
        );
        rec.upsert(
            "axes",
            Value::Array(ArrayValue::from_string_vec(self.axis_names())),
        );
        rec.upsert(
            "units",
            Value::Array(ArrayValue::from_string_vec(self.axis_units())),
        );
        rec.upsert(
            "pixelvalues",
            Value::Array(ArrayValue::from_f64_vec(self.pixel_values.clone())),
        );
        let world_values = if self.world_values.len() == 1 {
            vec![self.constant_world()]
        } else if self.has_reversed_pixel_interval() {
            // Casacore saves evaluated samples, including when the first
            // supplied pixel falls in a later interval of the table.
            self.pixel_values
                .iter()
                .zip(&self.world_values)
                .map(|(&pixel, &world)| {
                    self.scaled_world(
                        interpolate_samples(&self.pixel_values, &self.world_values, pixel)
                            .unwrap_or(world),
                    )
                })
                .collect()
        } else {
            self.world_values
                .iter()
                .map(|&value| self.scaled_world(value))
                .collect()
        };
        rec.upsert(
            "worldvalues",
            Value::Array(ArrayValue::from_f64_vec(world_values)),
        );
        rec
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_linear_tab() -> TabularCoordinate {
        // Linear table: pixel 0..4 -> world 100..500
        TabularCoordinate::new(
            vec![0.0, 1.0, 2.0, 3.0, 4.0],
            vec![100.0, 200.0, 300.0, 400.0, 500.0],
            "Velocity",
            "km/s",
        )
    }

    #[test]
    fn exact_table_values() {
        let coord = make_linear_tab();
        assert_eq!(coord.to_world(&[0.0]).unwrap(), vec![100.0]);
        assert_eq!(coord.to_world(&[2.0]).unwrap(), vec![300.0]);
        assert_eq!(coord.to_world(&[4.0]).unwrap(), vec![500.0]);
    }

    #[test]
    fn interpolation() {
        let coord = make_linear_tab();
        let world = coord.to_world(&[1.5]).unwrap();
        assert!((world[0] - 250.0).abs() < 1e-10);
    }

    #[test]
    fn reverse_interpolation() {
        let coord = make_linear_tab();
        let pixel = coord.to_pixel(&[250.0]).unwrap();
        assert!((pixel[0] - 1.5).abs() < 1e-10);
    }

    #[test]
    fn roundtrip() {
        let coord = make_linear_tab();
        let pixel = vec![2.7];
        let world = coord.to_world(&pixel).unwrap();
        let back = coord.to_pixel(&world).unwrap();
        assert!((back[0] - pixel[0]).abs() < 1e-10);
    }

    #[test]
    fn non_linear_table() {
        // Non-linear spacing
        let coord = TabularCoordinate::new(
            vec![0.0, 1.0, 2.0, 3.0],
            vec![0.0, 1.0, 4.0, 9.0], // quadratic-like
            "X",
            "",
        );
        // At pixel 1.5, world should be interpolated between 1.0 and 4.0 = 2.5
        let world = coord.to_world(&[1.5]).unwrap();
        assert!((world[0] - 2.5).abs() < 1e-10);
    }

    #[test]
    fn extrapolation() {
        let coord = make_linear_tab();
        // Extrapolate beyond table
        let world = coord.to_world(&[5.0]).unwrap();
        assert!((world[0] - 600.0).abs() < 1e-10);
    }

    #[test]
    fn dimension_mismatch() {
        let coord = make_linear_tab();
        assert!(coord.to_world(&[]).is_err());
        assert!(coord.to_pixel(&[1.0, 2.0]).is_err());
    }

    #[test]
    fn trait_methods() {
        let coord = make_linear_tab();
        assert_eq!(coord.coordinate_type(), CoordinateType::Tabular);
        assert_eq!(coord.n_pixel_axes(), 1);
        assert_eq!(coord.n_world_axes(), 1);
        assert_eq!(coord.axis_names(), vec!["Velocity"]);
        assert_eq!(coord.axis_units(), vec!["km/s"]);
        assert_eq!(coord.reference_value(), vec![100.0]);
        assert_eq!(coord.reference_pixel(), vec![0.0]);
    }

    #[test]
    fn to_record_has_fields() {
        let coord = make_linear_tab();
        let rec = coord.to_record();
        assert!(rec.get("pixelvalues").is_some());
        assert!(rec.get("worldvalues").is_some());
        assert!(rec.get("axes").is_some());
    }

    #[test]
    fn record_roundtrip() {
        let coord = make_linear_tab();
        let restored = TabularCoordinate::from_record(&coord.to_record()).unwrap();

        assert_eq!(restored.pixel_values(), coord.pixel_values());
        assert_eq!(restored.world_values(), coord.world_values());
        assert_eq!(restored.axis_names(), coord.axis_names());
        assert_eq!(restored.axis_units(), coord.axis_units());
    }
}
