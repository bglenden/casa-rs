// SPDX-License-Identifier: LGPL-3.0-or-later
//! One-axis spectral (frequency/velocity) coordinate.
//!
//! [`SpectralCoordinate`] implements a one-axis spectral coordinate in a
//! specified reference frame. It supports both the simple linear WCS form and
//! the tabulated lookup-table form used by C++ `SpectralCoordinate`.

use std::str::FromStr;

use casa_types::measures::direction_to_record;
use casa_types::measures::epoch_to_record;
use casa_types::measures::frequency::{FrequencyRef, MFrequency};
use casa_types::measures::position_to_record;
use casa_types::measures::{
    MeasFrame, direction_from_record, epoch_from_record, position_from_record,
};
use casa_types::quanta::Unit;
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};

use crate::coordinate::{Coordinate, CoordinateType};
use crate::error::CoordinateError;
use crate::record_utils::{
    get_optional_i32, get_optional_string, get_optional_vec_f64, get_optional_vec_string,
    get_required_f64, get_required_vec_f64,
};
use crate::tabular::{TabularEdit, edit_table};

#[derive(Debug, Clone)]
enum SpectralMapping {
    Linear {
        crval: f64,
        cdelt: f64,
        crpix: f64,
    },
    Tabular {
        pixel_values: Vec<f64>,
        world_values: Vec<f64>,
        crval: f64,
        cdelt: f64,
        crpix: f64,
    },
}

/// Index of casacore's nearest listed rest frequency (first on ties).
pub(crate) fn closest_rest_frequency(values: &[f64], target: f64) -> Option<usize> {
    values
        .iter()
        .copied()
        .enumerate()
        .min_by(|(_, a), (_, b)| (a - target).abs().total_cmp(&(b - target).abs()))
        .map(|(index, _)| index)
}

fn frequency_unit_scale(unit: &str) -> Option<f64> {
    if unit == "Hz" {
        return Some(1.0);
    }
    let unit = Unit::new(unit).ok()?;
    let hz = Unit::new("Hz").ok()?;
    let scale = unit.val().factor / hz.val().factor;
    (unit.conformant(&hz) && scale.is_normal() && scale > 0.0).then_some(scale)
}

#[derive(Debug, Clone)]
struct SpectralConversion {
    frequency_ref: FrequencyRef,
    frame: MeasFrame,
}

/// A one-axis spectral coordinate with linear or tabulated pixel-to-world mapping.
///
/// Stores the frequency reference frame, a rest frequency for velocity
/// conversions, axis metadata, and either the standard FITS WCS linear
/// parameters or an explicit spectral lookup table. This corresponds to C++
/// `SpectralCoordinate`, including its tabulated non-linear form.
///
/// Corresponds to C++ `SpectralCoordinate`.
#[derive(Debug, Clone)]
pub struct SpectralCoordinate {
    /// The velocity reference frame (LSRK, BARY, TOPO, etc.).
    frequency_ref: FrequencyRef,
    /// Whether the record declares no frame: C++ `MFrequency::Undefined`,
    /// which CASA writes for a `specmode='cubedata'` image. Its frequencies
    /// are the data's as observed, and arithmetic treats them as `frequency_ref`.
    undefined_frame: bool,
    /// Optional CASA conversion-layer state for world-coordinate display.
    conversion: Option<SpectralConversion>,
    /// Active rest frequency (Hz for new coordinates, stored unit on restore).
    rest_frequency: f64,
    /// Every rest frequency the coordinate lists, the active one among them
    /// (C++ `restFrequencies()`).
    rest_frequencies: Vec<f64>,
    /// Pixel-to-world mapping state.
    mapping: SpectralMapping,
    /// The world axis name.
    name: String,
    /// The world axis unit string.
    unit: String,
    /// The stored unit's scale to Hz, if it is a supported frequency unit.
    unit_scale_hz: Option<f64>,
}

impl SpectralCoordinate {
    /// Creates a new spectral coordinate.
    ///
    /// - `frequency_ref`: the velocity reference frame.
    /// - `crval`: reference frequency in Hz.
    /// - `cdelt`: frequency increment per pixel in Hz.
    /// - `crpix`: reference pixel position.
    /// - `rest_frequency`: the rest frequency in Hz (for velocity conversions).
    pub fn new(
        frequency_ref: FrequencyRef,
        crval: f64,
        cdelt: f64,
        crpix: f64,
        rest_frequency: f64,
    ) -> Self {
        Self {
            frequency_ref,
            undefined_frame: frequency_ref == FrequencyRef::Undefined,
            conversion: None,
            rest_frequency,
            rest_frequencies: vec![rest_frequency],
            mapping: SpectralMapping::Linear {
                crval,
                cdelt,
                crpix,
            },
            name: "Frequency".into(),
            unit: "Hz".into(),
            unit_scale_hz: Some(1.0),
        }
    }

    /// Creates a tabulated spectral coordinate from explicit lookup tables.
    ///
    /// This mirrors the non-linear record form used by C++
    /// `SpectralCoordinate` when a `tabular` sub-record contains
    /// `pixelvalues/worldvalues`.
    pub fn from_tabular(
        frequency_ref: FrequencyRef,
        pixel_values: Vec<f64>,
        world_values: Vec<f64>,
        crval: f64,
        cdelt: f64,
        crpix: f64,
        rest_frequency: f64,
    ) -> Result<Self, CoordinateError> {
        if pixel_values.len() != world_values.len() || pixel_values.len() < 2 {
            return Err(CoordinateError::InvalidRecord(
                "tabular spectral coordinates require matching pixel/world tables with at least 2 entries"
                    .into(),
            ));
        }
        Ok(Self {
            frequency_ref,
            undefined_frame: frequency_ref == FrequencyRef::Undefined,
            conversion: None,
            rest_frequency,
            rest_frequencies: vec![rest_frequency],
            mapping: SpectralMapping::Tabular {
                pixel_values,
                world_values,
                crval,
                cdelt,
                crpix,
            },
            name: "Frequency".into(),
            unit: "Hz".into(),
            unit_scale_hz: Some(1.0),
        })
    }

    /// Sets the world axis unit string. Returns `self` for chaining.
    pub fn with_unit(mut self, unit: impl Into<String>) -> Self {
        let unit = unit.into();
        self.unit_scale_hz = frequency_unit_scale(&unit);
        self.unit = unit;
        self
    }

    /// Convert a value in the coordinate's stored frequency unit to Hz.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored unit is not a supported frequency unit.
    pub fn value_in_hz(&self, value: f64) -> Result<f64, CoordinateError> {
        let scale = self.unit_scale_hz.ok_or_else(|| {
            CoordinateError::InvalidRecord(format!(
                "invalid spectral frequency unit {:?}",
                self.unit
            ))
        })?;
        Ok(value * scale)
    }

    /// Convert a frequency in Hz to the coordinate's stored unit.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored unit is not a supported frequency unit.
    pub fn value_from_hz(&self, value: f64) -> Result<f64, CoordinateError> {
        let scale = self.unit_scale_hz.ok_or_else(|| {
            CoordinateError::InvalidRecord(format!(
                "invalid spectral frequency unit {:?}",
                self.unit
            ))
        })?;
        Ok(value / scale)
    }

    /// Return the active rest frequency in Hz.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored unit is not a supported frequency unit.
    pub fn rest_frequency_hz(&self) -> Result<f64, CoordinateError> {
        self.value_in_hz(self.rest_frequency)
    }

    /// Return the world-frame frequency at a pixel, in Hz.
    ///
    /// # Errors
    ///
    /// Returns an error if the unit or pixel-to-world conversion is invalid.
    pub fn world_frequency_hz(&self, pixel: f64) -> Result<f64, CoordinateError> {
        self.value_in_hz(self.to_world(&[pixel])?[0])
    }

    /// Return the spectral increment in the world frame, expressed in Hz.
    ///
    /// # Errors
    ///
    /// Returns an error if the unit or reference-frame conversion is invalid.
    pub fn world_increment_hz(&self) -> Result<f64, CoordinateError> {
        self.value_in_hz(self.convert_native_to_world(self.increment()[0])?)
    }

    /// Linear FITS frequency axis `(reference pixel, reference Hz, increment Hz)`.
    ///
    /// Casacore represents a non-linear table in FITS by the line through
    /// its first two sampled frequencies, anchored at the first pixel.
    ///
    /// # Errors
    ///
    /// Returns an error if the unit or pixel-to-world conversion is invalid.
    pub fn fits_linearized_axis_hz(&self) -> Result<(f64, f64, f64), CoordinateError> {
        if let SpectralMapping::Tabular { pixel_values, .. } = &self.mapping {
            let first = self.world_frequency_hz(pixel_values[0])?;
            let second = self.world_frequency_hz(pixel_values[1])?;
            Ok((pixel_values[0], first, second - first))
        } else {
            let pixel = self.reference_pixel()[0];
            Ok((
                pixel,
                self.world_frequency_hz(pixel)?,
                self.world_increment_hz()?,
            ))
        }
    }

    /// Apply a tabular coordinate edit and canonicalize the resulting table
    /// as casacore does when it saves and restores a spectral coordinate.
    pub(crate) fn edit_tabular(&mut self, edit: TabularEdit) -> Result<f64, CoordinateError> {
        let SpectralMapping::Tabular {
            pixel_values,
            world_values,
            crval,
            cdelt,
            crpix,
        } = &mut self.mapping
        else {
            return Err(CoordinateError::InvalidRecord(
                "spectral coordinate is not tabular".into(),
            ));
        };
        let updated = edit_table(pixel_values, world_values, *crval, *cdelt, *crpix, edit)?;
        *crval = updated.values[0];
        *cdelt = updated.increment;
        *crpix = pixel_values[0];
        *world_values = updated.values;
        Ok(updated.old)
    }

    /// Return the pixel for a world-frame frequency expressed in Hz.
    ///
    /// # Errors
    ///
    /// Returns an error if the unit or world-to-pixel conversion is invalid.
    pub fn pixel_at_frequency_hz(&self, frequency_hz: f64) -> Result<f64, CoordinateError> {
        Ok(self.to_pixel(&[self.value_from_hz(frequency_hz)?])?[0])
    }

    /// Sets the CASA conversion-layer frame used for displayed world coordinates.
    pub fn with_conversion(mut self, frequency_ref: FrequencyRef, frame: MeasFrame) -> Self {
        self.conversion = Some(SpectralConversion {
            frequency_ref,
            frame,
        });
        self
    }

    /// Declares no frame for the axis, as CASA's `MFrequency::Undefined`
    /// does for a `specmode='cubedata'` image: the record's `system` reads
    /// `"Undefined"`, while frequencies keep meaning those of
    /// [`Self::frequency_ref`]. Returns `self` for chaining.
    pub fn with_undefined_frame(mut self) -> Self {
        self.undefined_frame = true;
        self
    }

    /// Whether the axis declares no frame (C++ `MFrequency::Undefined`); see
    /// [`Self::with_undefined_frame`].
    pub fn has_undefined_frame(&self) -> bool {
        self.undefined_frame
    }

    /// Sets the world axis name. Returns `self` for chaining.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Returns the velocity reference frame.
    pub fn frequency_ref(&self) -> FrequencyRef {
        self.frequency_ref
    }

    /// Whether the native pixel-to-frequency mapping uses FITS linear parameters.
    ///
    /// Returns `false` for an explicit spectral lookup table, even when its
    /// reference value, pixel and increment equal those of a linear coordinate.
    /// This describes the native mapping before optional reference-frame conversion.
    pub fn has_linear_mapping(&self) -> bool {
        matches!(self.mapping, SpectralMapping::Linear { .. })
    }

    /// Returns the CASA conversion-layer frequency frame, if configured.
    pub fn conversion_frequency_ref(&self) -> Option<FrequencyRef> {
        self.conversion
            .as_ref()
            .map(|conversion| conversion.frequency_ref)
    }

    /// Returns the measures frame of the CASA conversion layer (its epoch,
    /// position and direction), if configured.
    pub fn conversion_frame(&self) -> Option<&MeasFrame> {
        self.conversion.as_ref().map(|conversion| &conversion.frame)
    }

    /// Returns the world-coordinate frame presented by pixel/world conversion.
    pub fn world_frequency_ref(&self) -> FrequencyRef {
        self.conversion_frequency_ref()
            .unwrap_or(self.frequency_ref)
    }

    /// Returns the active rest frequency. New coordinates take this value in
    /// Hz; a coordinate restored from a CASA record retains its stored world
    /// unit, which may differ from Hz (#977).
    pub fn rest_frequency(&self) -> f64 {
        self.rest_frequency
    }

    /// Sets the rest frequencies the coordinate lists, the first active,
    /// as C++ `setRestFrequencies(frequencies, 0)` does. An empty list
    /// leaves the coordinate as it is. Returns `self` for chaining.
    pub fn with_rest_frequencies(mut self, frequencies: Vec<f64>) -> Self {
        if let Some(&active) = frequencies.first() {
            self.rest_frequency = active;
            self.rest_frequencies = frequencies;
        }
        self
    }

    /// Returns every listed rest frequency in the same unit as
    /// [`Self::rest_frequency`] (C++ `restFrequencies()`).
    pub fn rest_frequencies(&self) -> &[f64] {
        &self.rest_frequencies
    }

    /// Reconstructs a spectral coordinate from a serialized record.
    pub fn from_record(rec: &RecordValue) -> Result<Self, CoordinateError> {
        let undefined_frame;
        let frequency_ref = if let Some(name) =
            get_optional_string(rec, "frequency_ref").or_else(|| get_optional_string(rec, "system"))
        {
            undefined_frame = name.eq_ignore_ascii_case("Undefined");
            parse_frequency_ref_name(&name).ok_or_else(|| {
                CoordinateError::InvalidRecord(format!("invalid spectral frequency_ref: {name}"))
            })?
        } else if let Some(code) = get_optional_i32(rec, "frequency_ref") {
            undefined_frame = code == FrequencyRef::Undefined.casacore_code();
            FrequencyRef::from_casacore_code(code).ok_or_else(|| {
                CoordinateError::InvalidRecord(format!(
                    "invalid spectral frequency_ref code {code}"
                ))
            })?
        } else {
            return Err(CoordinateError::InvalidRecord(
                "missing or invalid frequency_ref".into(),
            ));
        };

        let mut parameter_record = rec;
        let mut lookup_tables = None;
        if let Some(Value::Record(wcs)) = rec.get("wcs") {
            parameter_record = wcs;
        } else if let Some(Value::Record(tabular)) = rec.get("tabular") {
            let pixelvalues = get_optional_vec_f64(tabular, "pixelvalues").unwrap_or_default();
            let worldvalues = get_optional_vec_f64(tabular, "worldvalues").unwrap_or_default();
            if !pixelvalues.is_empty() || !worldvalues.is_empty() {
                lookup_tables = Some((pixelvalues, worldvalues));
            }
            parameter_record = tabular;
        }

        let mut crval = get_required_vec_f64(parameter_record, "crval")?;
        let mut cdelt = get_required_vec_f64(parameter_record, "cdelt")?;
        let mut crpix = get_required_vec_f64(parameter_record, "crpix")?;
        if crval.len() != 1 || cdelt.len() != 1 || crpix.len() != 1 {
            return Err(CoordinateError::InvalidRecord(
                "spectral coordinate expects single-valued crval/cdelt/crpix".into(),
            ));
        }

        let rest_frequency = get_required_f64(rec, "restfreq")?;
        if !rest_frequency.is_finite() || rest_frequency < 0.0 {
            return Err(CoordinateError::InvalidRecord(
                "invalid rest frequency".into(),
            ));
        }
        let axis_unit = get_optional_string(rec, "unit").or_else(|| {
            get_optional_vec_string(parameter_record, "units")
                .and_then(|units| units.into_iter().next())
        });
        let mut coord = if let Some((mut pixel_values, mut world_values)) = lookup_tables {
            // Casacore's nested tabular coordinate is in Hz, while the
            // surrounding spectral coordinate may display another unit.
            let table_unit = get_optional_vec_string(parameter_record, "units")
                .and_then(|units| units.into_iter().next())
                .unwrap_or_else(|| "Hz".into());
            let from = frequency_unit_scale(&table_unit).ok_or_else(|| {
                CoordinateError::InvalidRecord(format!(
                    "invalid tabular spectral unit {table_unit:?}"
                ))
            })?;
            let to = frequency_unit_scale(axis_unit.as_deref().unwrap_or("Hz"))
                .ok_or_else(|| CoordinateError::InvalidRecord("invalid spectral unit".into()))?;
            let factor = from / to;
            crval[0] *= factor;
            cdelt[0] *= factor;
            for value in &mut world_values {
                *value *= factor;
            }
            if pixel_values.len() >= 2 && world_values.len() == pixel_values.len() {
                let last = pixel_values.len() - 1;
                if pixel_values[last] == pixel_values[0] {
                    return Err(CoordinateError::InvalidRecord(
                        "tabular spectral pixel endpoints coincide".into(),
                    ));
                }
                // Casacore rebuilds a spectral lookup with consecutive
                // channel indices, then restores the table's reference pixel.
                let original_reference_pixel = pixel_values[0];
                crval[0] = world_values[0];
                cdelt[0] = (world_values[last] - world_values[0]) / last as f64;
                crpix[0] = original_reference_pixel;
                for (index, (pixel, world)) in
                    pixel_values.iter_mut().zip(&mut world_values).enumerate()
                {
                    *pixel = index as f64;
                    *world -= cdelt[0] * original_reference_pixel;
                }
            }
            Self::from_tabular(
                frequency_ref,
                pixel_values,
                world_values,
                crval[0],
                cdelt[0],
                crpix[0],
                rest_frequency,
            )?
        } else {
            Self::new(frequency_ref, crval[0], cdelt[0], crpix[0], rest_frequency)
        };
        if let Some(name) = get_optional_string(rec, "name").or_else(|| {
            get_optional_vec_string(parameter_record, "axes")
                .and_then(|names| names.into_iter().next())
        }) {
            coord = coord.with_name(name);
        }
        if let Some(unit) = axis_unit {
            coord = coord.with_unit(unit);
        }
        if let Some(conversion) = parse_conversion(rec) {
            coord = coord.with_conversion(conversion.frequency_ref, conversion.frame);
        }
        coord.undefined_frame = undefined_frame;
        // C++ `restoreRestFrequencies` clamps the optional list and selects
        // the listed frequency nearest the saved `restfreq` selector.
        if let Some(field) = rec.get("restfreqs") {
            if matches!(field, Value::Array(array) if array.shape().len() != 1) {
                return Err(CoordinateError::InvalidRecord(
                    "invalid rest frequency list".into(),
                ));
            }
            let list = get_optional_vec_f64(rec, "restfreqs").ok_or_else(|| {
                CoordinateError::InvalidRecord("invalid rest frequency list".into())
            })?;
            if list.is_empty() || list.iter().any(|value| !value.is_finite()) {
                return Err(CoordinateError::InvalidRecord(
                    "invalid rest frequency list".into(),
                ));
            }
            coord.rest_frequencies = list.into_iter().map(|value| value.max(0.0)).collect();
            if let Some(index) = closest_rest_frequency(&coord.rest_frequencies, rest_frequency) {
                coord.rest_frequency = coord.rest_frequencies[index];
            }
        }
        Ok(coord)
    }
}

fn parse_frequency_ref_name(name: &str) -> Option<FrequencyRef> {
    if name.eq_ignore_ascii_case("Undefined") {
        // CASA `specmode='cubedata'` serializes a valid native-frequency
        // axis with `system="Undefined"`; retain its established handling.
        Some(FrequencyRef::TOPO)
    } else {
        FrequencyRef::from_str(name).ok()
    }
}

impl Coordinate for SpectralCoordinate {
    fn coordinate_type(&self) -> CoordinateType {
        CoordinateType::Spectral
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
        let world = match &self.mapping {
            SpectralMapping::Linear {
                crval,
                cdelt,
                crpix,
            } => crval + cdelt * (pixel[0] - crpix),
            SpectralMapping::Tabular {
                pixel_values,
                world_values,
                ..
            } => interpolate(pixel_values, world_values, pixel[0])?,
        };
        Ok(vec![self.convert_native_to_world(world)?])
    }

    fn to_pixel(&self, world: &[f64]) -> Result<Vec<f64>, CoordinateError> {
        if world.len() != 1 {
            return Err(CoordinateError::DimensionMismatch {
                expected: 1,
                got: world.len(),
            });
        }
        let native_world = self.convert_world_to_native(world[0])?;
        let pixel = match &self.mapping {
            SpectralMapping::Linear {
                crval,
                cdelt,
                crpix,
            } => {
                if cdelt.abs() < 1e-300 {
                    return Err(CoordinateError::ConversionFailed(
                        "zero spectral increment".into(),
                    ));
                }
                crpix + (native_world - crval) / cdelt
            }
            SpectralMapping::Tabular {
                pixel_values,
                world_values,
                ..
            } => interpolate(world_values, pixel_values, native_world)?,
        };
        Ok(vec![pixel])
    }

    fn reference_value(&self) -> Vec<f64> {
        vec![match &self.mapping {
            SpectralMapping::Linear { crval, .. } | SpectralMapping::Tabular { crval, .. } => {
                *crval
            }
        }]
    }

    fn reference_pixel(&self) -> Vec<f64> {
        vec![match &self.mapping {
            SpectralMapping::Linear { crpix, .. } | SpectralMapping::Tabular { crpix, .. } => {
                *crpix
            }
        }]
    }

    fn increment(&self) -> Vec<f64> {
        vec![match &self.mapping {
            SpectralMapping::Linear { cdelt, .. } | SpectralMapping::Tabular { cdelt, .. } => {
                *cdelt
            }
        }]
    }

    fn axis_names(&self) -> Vec<String> {
        vec![self.name.clone()]
    }

    fn axis_units(&self) -> Vec<String> {
        vec![self.unit.clone()]
    }
}

impl SpectralCoordinate {
    /// The record's `system`: the frame, or `"Undefined"` for an axis that
    /// declares none.
    fn system_name(&self) -> &'static str {
        if self.undefined_frame {
            "Undefined"
        } else {
            self.frequency_ref.as_str()
        }
    }

    pub(crate) fn to_record(&self) -> RecordValue {
        let mut rec = RecordValue::default();
        rec.upsert("version", Value::Scalar(ScalarValue::Int32(2)));
        rec.upsert(
            "system",
            Value::Scalar(ScalarValue::String(self.system_name().into())),
        );
        rec.upsert(
            "restfreq",
            Value::Scalar(ScalarValue::Float64(self.rest_frequency)),
        );
        rec.upsert(
            "restfreqs",
            Value::Array(ArrayValue::from_f64_vec(self.rest_frequencies.clone())),
        );
        rec.upsert("velType", Value::Scalar(ScalarValue::Int32(0)));
        rec.upsert("nativeType", Value::Scalar(ScalarValue::Int32(0)));
        rec.upsert("velUnit", Value::Scalar(ScalarValue::String("km/s".into())));
        rec.upsert("waveUnit", Value::Scalar(ScalarValue::String("mm".into())));
        rec.upsert(
            "formatUnit",
            Value::Scalar(ScalarValue::String(String::new())),
        );
        rec.upsert(
            "unit",
            Value::Scalar(ScalarValue::String(self.unit.clone())),
        );
        rec.upsert(
            "name",
            Value::Scalar(ScalarValue::String(self.name.clone())),
        );

        match &self.mapping {
            SpectralMapping::Linear {
                crval,
                cdelt,
                crpix,
            } => {
                let mut wcs = RecordValue::default();
                wcs.upsert("crval", Value::Scalar(ScalarValue::Float64(*crval)));
                wcs.upsert("crpix", Value::Scalar(ScalarValue::Float64(*crpix)));
                wcs.upsert("cdelt", Value::Scalar(ScalarValue::Float64(*cdelt)));
                wcs.upsert("pc", Value::Scalar(ScalarValue::Float64(1.0)));
                wcs.upsert("ctype", Value::Scalar(ScalarValue::String("FREQ".into())));
                rec.upsert("wcs", Value::Record(wcs));
            }
            SpectralMapping::Tabular {
                pixel_values,
                world_values,
                crval,
                cdelt,
                crpix,
            } => {
                let to_hz = self.unit_scale_hz.unwrap_or(1.0);
                let mut tabular = RecordValue::default();
                tabular.upsert(
                    "crval",
                    Value::Array(ArrayValue::from_f64_vec(vec![*crval * to_hz])),
                );
                tabular.upsert(
                    "cdelt",
                    Value::Array(ArrayValue::from_f64_vec(vec![*cdelt * to_hz])),
                );
                tabular.upsert(
                    "crpix",
                    Value::Array(ArrayValue::from_f64_vec(vec![*crpix])),
                );
                tabular.upsert("pc", Value::Array(ArrayValue::from_f64_vec(vec![1.0])));
                tabular.upsert(
                    "axes",
                    Value::Array(ArrayValue::from_string_vec(vec![self.name.clone()])),
                );
                tabular.upsert(
                    "units",
                    Value::Array(ArrayValue::from_string_vec(vec!["Hz".into()])),
                );
                tabular.upsert(
                    "pixelvalues",
                    Value::Array(ArrayValue::from_f64_vec(pixel_values.clone())),
                );
                tabular.upsert(
                    "worldvalues",
                    Value::Array(ArrayValue::from_f64_vec(
                        world_values.iter().map(|value| value * to_hz).collect(),
                    )),
                );
                rec.upsert("tabular", Value::Record(tabular));
            }
        }

        if let Some(conversion) = &self.conversion
            && let (Some(direction), Some(position), Some(epoch)) = (
                conversion.frame.direction(),
                conversion.frame.position(),
                conversion.frame.epoch(),
            )
        {
            let mut conversion_record = RecordValue::default();
            conversion_record.upsert(
                "system",
                Value::Scalar(ScalarValue::String(
                    conversion.frequency_ref.as_str().into(),
                )),
            );
            conversion_record.upsert("direction", Value::Record(direction_to_record(direction)));
            conversion_record.upsert("position", Value::Record(position_to_record(position)));
            conversion_record.upsert("epoch", Value::Record(epoch_to_record(epoch)));
            rec.upsert("conversion", Value::Record(conversion_record));
        }

        rec
    }
}

impl SpectralCoordinate {
    fn convert_native_to_world(&self, native: f64) -> Result<f64, CoordinateError> {
        let Some(conversion) = &self.conversion else {
            return Ok(native);
        };
        let native_hz = self.value_in_hz(native)?;
        let converted_hz = MFrequency::new(native_hz, self.frequency_ref)
            .convert_to(conversion.frequency_ref, &conversion.frame)
            .map(|frequency| frequency.hz())
            .map_err(|err| {
                CoordinateError::ConversionFailed(format!(
                    "spectral frame conversion {} -> {} failed: {err}",
                    self.frequency_ref, conversion.frequency_ref
                ))
            })?;
        self.value_from_hz(converted_hz)
    }

    fn convert_world_to_native(&self, world: f64) -> Result<f64, CoordinateError> {
        let Some(conversion) = &self.conversion else {
            return Ok(world);
        };
        let world_hz = self.value_in_hz(world)?;
        let converted_hz = MFrequency::new(world_hz, conversion.frequency_ref)
            .convert_to(self.frequency_ref, &conversion.frame)
            .map(|frequency| frequency.hz())
            .map_err(|err| {
                CoordinateError::ConversionFailed(format!(
                    "spectral frame conversion {} -> {} failed: {err}",
                    conversion.frequency_ref, self.frequency_ref
                ))
            })?;
        self.value_from_hz(converted_hz)
    }
}

fn parse_conversion(rec: &RecordValue) -> Option<SpectralConversion> {
    let Value::Record(conversion) = rec.get("conversion")? else {
        return None;
    };
    let target = FrequencyRef::from_str(&get_optional_string(conversion, "system")?).ok()?;
    // An undefined conversion frame is a declaration, not a physical
    // conversion. It was ignored before Undefined became a FrequencyRef;
    // preserve the native coordinate rather than routing TOPO to Undefined.
    if target == FrequencyRef::Undefined {
        return None;
    }
    let Value::Record(direction_rec) = conversion.get("direction")? else {
        return None;
    };
    let Value::Record(position_rec) = conversion.get("position")? else {
        return None;
    };
    let Value::Record(epoch_rec) = conversion.get("epoch")? else {
        return None;
    };
    let direction = direction_from_record(direction_rec).ok()?;
    let position = position_from_record(position_rec).ok()?;
    let epoch = epoch_from_record(epoch_rec).ok()?;
    Some(SpectralConversion {
        frequency_ref: target,
        frame: MeasFrame::new()
            .with_direction(direction)
            .with_position(position)
            .with_epoch(epoch),
    })
}

fn interpolate(xs: &[f64], ys: &[f64], x: f64) -> Result<f64, CoordinateError> {
    let n = xs.len();
    if n < 2 || ys.len() != n {
        return Err(CoordinateError::ConversionFailed(
            "tabular interpolation requires matching lookup tables with at least 2 entries".into(),
        ));
    }

    let increasing = xs[n - 1] > xs[0];
    let idx = if increasing {
        match xs.binary_search_by(|probe| probe.partial_cmp(&x).unwrap()) {
            Ok(i) => return Ok(ys[i]),
            Err(i) => i,
        }
    } else {
        let rev: Vec<f64> = xs.iter().rev().copied().collect();
        match rev.binary_search_by(|probe| probe.partial_cmp(&x).unwrap()) {
            Ok(i) => return Ok(ys[n - 1 - i]),
            Err(i) => n - i,
        }
    };

    let lo = if idx == 0 { 0 } else { (idx - 1).min(n - 2) };
    let hi = lo + 1;
    let dx = xs[hi] - xs[lo];
    if dx.abs() < 1e-300 {
        return Err(CoordinateError::ConversionFailed(
            "duplicate values in tabular spectral coordinate".into(),
        ));
    }
    let t = (x - xs[lo]) / dx;
    Ok(ys[lo] + t * (ys[hi] - ys[lo]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_types::measures::direction::{DirectionRef, MDirection};
    use casa_types::measures::epoch::{EpochRef, MEpoch};
    use casa_types::measures::position::MPosition;

    fn conversion_frame() -> MeasFrame {
        MeasFrame::new()
            .with_direction(MDirection::from_angles(1.0, 0.5, DirectionRef::J2000))
            .with_position(MPosition::new_wgs84(
                -2.1200320498502676,
                0.7123949192959743,
                1021.0,
            ))
            .with_epoch(MEpoch::from_mjd(50_919.14846423176, EpochRef::UTC))
    }

    #[test]
    fn reference_pixel_gives_crval() {
        let coord = SpectralCoordinate::new(FrequencyRef::LSRK, 1.4e9, 1e6, 0.0, 1.42040575e9);
        let world = coord.to_world(&[0.0]).unwrap();
        assert!((world[0] - 1.4e9).abs() < 1.0);
    }

    #[test]
    fn linear_mapping() {
        let coord = SpectralCoordinate::new(FrequencyRef::TOPO, 1.0e9, 1e6, 100.0, 0.0);
        // 10 channels above reference
        let world = coord.to_world(&[110.0]).unwrap();
        assert!((world[0] - 1.01e9).abs() < 1.0);
    }

    #[test]
    fn roundtrip() {
        let coord = SpectralCoordinate::new(FrequencyRef::BARY, 1.42e9, -500e3, 512.0, 1.42e9);
        let pixel = [600.0];
        let world = coord.to_world(&pixel).unwrap();
        let back = coord.to_pixel(&world).unwrap();
        assert!((back[0] - pixel[0]).abs() < 1e-10);
    }

    #[test]
    fn dimension_mismatch() {
        let coord = SpectralCoordinate::new(FrequencyRef::LSRK, 1e9, 1e6, 0.0, 0.0);
        assert!(coord.to_world(&[1.0, 2.0]).is_err());
        assert!(coord.to_pixel(&[]).is_err());
    }

    #[test]
    fn trait_methods() {
        let coord = SpectralCoordinate::new(FrequencyRef::LSRK, 1e9, 1e6, 0.0, 1.42e9);
        assert_eq!(coord.coordinate_type(), CoordinateType::Spectral);
        assert_eq!(coord.n_pixel_axes(), 1);
        assert_eq!(coord.n_world_axes(), 1);
        assert_eq!(coord.axis_names(), vec!["Frequency"]);
        assert_eq!(coord.axis_units(), vec!["Hz"]);
        assert_eq!(coord.frequency_ref(), FrequencyRef::LSRK);
        assert!((coord.rest_frequency() - 1.42e9).abs() < 1.0);
    }

    #[test]
    fn with_unit() {
        let coord =
            SpectralCoordinate::new(FrequencyRef::LSRK, 1e9, 1e6, 0.0, 0.0).with_unit("GHz");
        assert_eq!(coord.axis_units(), vec!["GHz"]);
    }

    #[test]
    fn with_name() {
        let coord =
            SpectralCoordinate::new(FrequencyRef::LSRK, 1e9, 1e6, 0.0, 0.0).with_name("Velocity");
        assert_eq!(coord.axis_names(), vec!["Velocity"]);
    }

    /// C++ `setRestFrequencies(list, 0)` makes the first active, and the
    /// record keeps the list (`restfreqs`) beside it (`restfreq`).
    #[test]
    fn rest_frequency_lists_keep_the_first_active_through_the_record() {
        let coord = SpectralCoordinate::new(FrequencyRef::BARY, 1e9, 1e6, 0.0, 1.0e9)
            .with_rest_frequencies(vec![1.42e9, 1.6e9, 1.7e9]);
        assert_eq!(coord.rest_frequency(), 1.42e9);
        assert_eq!(coord.rest_frequencies(), [1.42e9, 1.6e9, 1.7e9]);
        let record = coord.to_record();
        let restored = SpectralCoordinate::from_record(&record).expect("restored");
        assert_eq!(restored.rest_frequency(), 1.42e9);
        assert_eq!(restored.rest_frequencies(), [1.42e9, 1.6e9, 1.7e9]);
        let single = SpectralCoordinate::new(FrequencyRef::LSRK, 1e9, 1e6, 0.0, 1.42e9);
        assert_eq!(single.rest_frequencies(), [1.42e9]);
        assert_eq!(
            single
                .clone()
                .with_rest_frequencies(Vec::new())
                .rest_frequencies(),
            [1.42e9]
        );
    }

    #[test]
    fn with_conversion_changes_world_frame() {
        let coord = SpectralCoordinate::new(FrequencyRef::LSRD, 1.15022e11, 6.25005e6, 0.0, 1.0)
            .with_conversion(FrequencyRef::LSRK, conversion_frame());
        assert_eq!(coord.frequency_ref(), FrequencyRef::LSRD);
        assert_eq!(coord.conversion_frequency_ref(), Some(FrequencyRef::LSRK));
        assert_eq!(coord.world_frequency_ref(), FrequencyRef::LSRK);
    }

    #[test]
    fn undefined_conversion_record_preserves_native_pixel_world_mapping() {
        let coordinate = SpectralCoordinate::new(FrequencyRef::TOPO, 1.4e9, 1000.0, 1.0, 1.4e9)
            .with_conversion(FrequencyRef::Undefined, conversion_frame());
        let restored = SpectralCoordinate::from_record(&coordinate.to_record()).unwrap();
        assert_eq!(restored.conversion_frequency_ref(), None);
        let world = restored.to_world(&[2.0]).unwrap();
        assert_eq!(world, vec![1_400_001_000.0]);
        assert_eq!(restored.to_pixel(&world).unwrap(), vec![2.0]);
    }

    #[test]
    fn to_record_has_fields() {
        let coord = SpectralCoordinate::new(FrequencyRef::LSRK, 1e9, 1e6, 0.0, 1.42e9);
        let rec = coord.to_record();
        assert!(rec.get("system").is_some());
        assert!(rec.get("restfreq").is_some());
        assert!(rec.get("wcs").is_some());
    }

    #[test]
    fn record_roundtrip() {
        let coord =
            SpectralCoordinate::new(FrequencyRef::BARY, 1.42e9, -5.0e5, 256.0, 1.42040575e9)
                .with_unit("GHz");
        let restored = SpectralCoordinate::from_record(&coord.to_record()).unwrap();

        assert_eq!(restored.frequency_ref(), FrequencyRef::BARY);
        assert_eq!(restored.reference_value(), vec![1.42e9]);
        assert_eq!(restored.reference_pixel(), vec![256.0]);
        assert_eq!(restored.increment(), vec![-5.0e5]);
        assert!((restored.rest_frequency() - 1.42040575e9).abs() < 1.0);
        assert_eq!(restored.axis_units(), vec!["GHz"]);
    }

    #[test]
    fn tabular_record_roundtrip() {
        let coord = SpectralCoordinate::from_tabular(
            FrequencyRef::TOPO,
            vec![0.0, 1.0, 3.0, 4.0],
            vec![1.41e9, 1.4105e9, 1.412e9, 1.413e9],
            1.41e9,
            5.0e5,
            0.0,
            1.42040575e9,
        )
        .unwrap()
        .with_name("Frequency")
        .with_unit("Hz");

        let world = coord.to_world(&[2.0]).unwrap();
        assert!((world[0] - 1.41125e9).abs() < 1.0);
        let back = coord.to_pixel(&world).unwrap();
        assert!((back[0] - 2.0).abs() < 1e-10);

        let restored = SpectralCoordinate::from_record(&coord.to_record()).unwrap();
        assert_eq!(restored.frequency_ref(), FrequencyRef::TOPO);
        assert_eq!(restored.axis_names(), vec!["Frequency"]);
        assert_eq!(restored.axis_units(), vec!["Hz"]);
        assert_eq!(restored.reference_value(), vec![1.41e9]);
        assert_eq!(restored.reference_pixel(), vec![0.0]);
        // Casacore rebuilds spectral tables on consecutive channels.
        assert_eq!(restored.increment(), vec![1.0e6]);
        let restored_world = restored.to_world(&[2.0]).unwrap();
        assert!((restored_world[0] - 1.412e9).abs() < 1.0);
    }

    #[test]
    fn conversion_record_roundtrip_applies_measure_frame_conversion() {
        let frame = conversion_frame();
        let coord =
            SpectralCoordinate::new(FrequencyRef::LSRD, 1.15022e11, 6.25005e6, 0.0, 1.152712e11)
                .with_conversion(FrequencyRef::LSRK, frame.clone());

        let native = 1.15022e11 + 2.0 * 6.25005e6;
        let expected = MFrequency::new(native, FrequencyRef::LSRD)
            .convert_to(FrequencyRef::LSRK, &frame)
            .unwrap()
            .hz();

        let world = coord.to_world(&[2.0]).unwrap();
        assert!((world[0] - expected).abs() < 1.0);

        let back = coord.to_pixel(&world).unwrap();
        assert!((back[0] - 2.0).abs() < 1e-10);

        let restored = SpectralCoordinate::from_record(&coord.to_record()).unwrap();
        assert_eq!(restored.frequency_ref(), FrequencyRef::LSRD);
        assert_eq!(
            restored.conversion_frequency_ref(),
            Some(FrequencyRef::LSRK)
        );
        let restored_world = restored.to_world(&[2.0]).unwrap();
        assert!((restored_world[0] - expected).abs() < 1.0);
    }

    #[test]
    fn from_record_parses_casa_style_tabular_lookup_tables() {
        let tabular = RecordValue::new(vec![
            casa_types::RecordField::new(
                "crval",
                Value::Array(ArrayValue::from_f64_vec(vec![1.41e9])),
            ),
            casa_types::RecordField::new(
                "crpix",
                Value::Array(ArrayValue::from_f64_vec(vec![0.0])),
            ),
            casa_types::RecordField::new(
                "cdelt",
                Value::Array(ArrayValue::from_f64_vec(vec![5.0e5])),
            ),
            casa_types::RecordField::new(
                "axes",
                Value::Array(ArrayValue::from_string_vec(vec!["Frequency".into()])),
            ),
            casa_types::RecordField::new(
                "units",
                Value::Array(ArrayValue::from_string_vec(vec!["Hz".into()])),
            ),
            casa_types::RecordField::new(
                "pixelvalues",
                Value::Array(ArrayValue::from_f64_vec(vec![0.0, 1.0, 3.0, 4.0])),
            ),
            casa_types::RecordField::new(
                "worldvalues",
                Value::Array(ArrayValue::from_f64_vec(vec![
                    1.41e9, 1.4105e9, 1.412e9, 1.413e9,
                ])),
            ),
        ]);
        let record = RecordValue::new(vec![
            casa_types::RecordField::new(
                "system",
                Value::Scalar(ScalarValue::String("TOPO".into())),
            ),
            casa_types::RecordField::new(
                "restfreq",
                Value::Scalar(ScalarValue::Float64(1.42040575e9)),
            ),
            casa_types::RecordField::new("tabular", Value::Record(tabular)),
        ]);

        let coord = SpectralCoordinate::from_record(&record).unwrap();
        assert_eq!(coord.frequency_ref(), FrequencyRef::TOPO);
        assert_eq!(coord.axis_names(), vec!["Frequency"]);
        assert_eq!(coord.axis_units(), vec!["Hz"]);
        assert_eq!(coord.reference_value(), vec![1.41e9]);
        assert_eq!(coord.reference_pixel(), vec![0.0]);
        assert_eq!(coord.increment(), vec![1.0e6]);
        let world = coord.to_world(&[2.0]).unwrap();
        assert!((world[0] - 1.412e9).abs() < 1.0);
        let pixel = coord.to_pixel(&[1.412e9]).unwrap();
        assert!((pixel[0] - 2.0).abs() < 1e-10);
    }

    #[test]
    fn from_record_accepts_casa_cubedata_undefined_system() {
        let wcs = RecordValue::new(vec![
            casa_types::RecordField::new(
                "crval",
                Value::Array(ArrayValue::from_f64_vec(vec![6.66740920955153e9])),
            ),
            casa_types::RecordField::new(
                "crpix",
                Value::Array(ArrayValue::from_f64_vec(vec![0.0])),
            ),
            casa_types::RecordField::new(
                "cdelt",
                Value::Array(ArrayValue::from_f64_vec(vec![976.5625])),
            ),
            casa_types::RecordField::new(
                "ctype",
                Value::Array(ArrayValue::from_string_vec(vec!["FREQ".into()])),
            ),
        ]);
        let record = RecordValue::new(vec![
            casa_types::RecordField::new(
                "system",
                Value::Scalar(ScalarValue::String("Undefined".into())),
            ),
            casa_types::RecordField::new(
                "restfreq",
                Value::Scalar(ScalarValue::Float64(6.668518e9)),
            ),
            casa_types::RecordField::new(
                "name",
                Value::Scalar(ScalarValue::String("Frequency".into())),
            ),
            casa_types::RecordField::new("unit", Value::Scalar(ScalarValue::String("Hz".into()))),
            casa_types::RecordField::new("wcs", Value::Record(wcs)),
        ]);

        let coord = SpectralCoordinate::from_record(&record).unwrap();
        assert_eq!(coord.frequency_ref(), FrequencyRef::TOPO);
        assert_eq!(coord.axis_names(), vec!["Frequency"]);
        // The axis keeps declaring no frame when written back.
        assert!(coord.has_undefined_frame());
        assert!(matches!(
            coord.to_record().get("system"),
            Some(Value::Scalar(ScalarValue::String(system))) if system == "Undefined"
        ));
        let mut numeric = record.clone();
        numeric.remove("system");
        numeric.upsert("frequency_ref", Value::Scalar(ScalarValue::Int32(64)));
        let numeric = SpectralCoordinate::from_record(&numeric).unwrap();
        assert_eq!(numeric.frequency_ref(), FrequencyRef::Undefined);
        assert!(numeric.has_undefined_frame());
        assert!(matches!(
            numeric.to_record().get("system"),
            Some(Value::Scalar(ScalarValue::String(system))) if system == "Undefined"
        ));
        let explicit = SpectralCoordinate::new(FrequencyRef::Undefined, 1.0e9, 1.0e6, 0.0, 0.0);
        assert!(explicit.has_undefined_frame());
        let tabulated = SpectralCoordinate::from_tabular(
            FrequencyRef::Undefined,
            vec![0.0, 1.0],
            vec![1.0e9, 1.1e9],
            1.0e9,
            1.0e6,
            0.0,
            0.0,
        )
        .unwrap();
        assert!(tabulated.has_undefined_frame());
        let declared = SpectralCoordinate::new(FrequencyRef::TOPO, 1.0e9, 1.0e6, 0.0, 0.0);
        assert!(!declared.has_undefined_frame());
        assert!(matches!(
            declared.with_undefined_frame().to_record().get("system"),
            Some(Value::Scalar(ScalarValue::String(system))) if system == "Undefined"
        ));
    }
}
