// SPDX-License-Identifier: LGPL-3.0-or-later
//! Rebuilding sampled spectral axes for coordinate and image sections.

use casa_core::{ArrayValue, RecordValue, Value};

use crate::CoordinateError;
use crate::tabular::scaled_world;

use super::{
    SpectralCoordinate, SpectralMapping, frequency_unit_scale, get_optional_vec_string, interpolate,
};

impl SpectralCoordinate {
    /// Resample a nonlinear spectral axis for a coordinate subimage.
    ///
    /// Origins and source channel indices use casacore's single-precision
    /// pixel arithmetic. The new table is in the native frame and in Hz.
    /// Linear axes retain their WCS and return `None`.
    ///
    /// # Errors
    /// Returns an error for an invalid shape, allocation, or conversion.
    pub fn subimage_samples(
        &self,
        origin: f32,
        length: Option<i32>,
    ) -> Result<Option<Self>, CoordinateError> {
        let SpectralMapping::Tabular {
            pixel_values,
            world_values,
            crval,
            ..
        } = &self.mapping
        else {
            return Ok(None);
        };
        let length = length.map_or(world_values.len() as i64, i64::from);
        if length <= 0 {
            return Err(CoordinateError::InvalidRecord(
                "TabularCoordinate::TabularCoordinate - illegal table (length 0 or n(pixelvalues) != n(worldvalues)".into(),
            ));
        }
        if length > (1_i64 << f32::MANTISSA_DIGITS) + 1 {
            return Err(CoordinateError::InvalidRecord(
                "spectral subimage pixel indices are indistinguishable at single precision".into(),
            ));
        }
        let count = length as usize;
        let mut worlds = Vec::new();
        worlds.try_reserve_exact(count).map_err(|_| {
            CoordinateError::InvalidRecord("cannot allocate the spectral subimage samples".into())
        })?;
        for channel in 0..count {
            let pixel = f64::from(origin + channel as f32);
            let world = interpolate(pixel_values, world_values, pixel)?;
            let frequency = self.value_in_hz(scaled_world(world, *crval, self.linear_transform))?;
            worlds.push(frequency);
        }
        let mut result =
            Self::from_channel_frequencies(self.frequency_ref, worlds, self.rest_frequency_hz()?)?;
        result.conversion = self.conversion.clone();
        result.undefined_frame = self.undefined_frame;
        result.native_type = self.native_type;
        Ok(Some(result))
    }

    /// Rebuild a tabular spectral record for a unit-stride image section.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored coordinate or lookup table is invalid.
    pub fn subimage_tabular_record(
        record: &mut RecordValue,
        start: usize,
        length: usize,
    ) -> Result<bool, CoordinateError> {
        Self::subimage_tabular_record_with_stride(record, start, length, 1)
    }

    /// Rebuild a tabular spectral record for a strided image section.
    ///
    /// # Errors
    /// Returns an error if the lookup table or section is invalid.
    pub fn subimage_tabular_record_with_stride(
        record: &mut RecordValue,
        start: usize,
        length: usize,
        stride: usize,
    ) -> Result<bool, CoordinateError> {
        let Some(Value::Record(table)) = record.get("tabular") else {
            return Ok(false);
        };
        if length == 0 || stride == 0 {
            return Err(CoordinateError::InvalidRecord(
                "tabular spectral image section needs a positive length and stride".into(),
            ));
        }
        let table_unit = get_optional_vec_string(table, "units")
            .and_then(|units| units.into_iter().next())
            .unwrap_or_else(|| "Hz".into());
        let coordinate = Self::from_record(record)?;
        let SpectralMapping::Tabular {
            pixel_values,
            world_values,
            ..
        } = &coordinate.mapping
        else {
            return Ok(false);
        };
        let factor = coordinate
            .unit_scale_hz
            .ok_or_else(|| CoordinateError::InvalidRecord("invalid spectral unit".into()))?
            / frequency_unit_scale(&table_unit).ok_or_else(|| {
                CoordinateError::InvalidRecord("invalid tabular spectral unit".into())
            })?;
        let count = if world_values.len() == 1 { 1 } else { length };
        let values = (0..count)
            .map(|pixel| {
                let source = pixel
                    .checked_mul(stride)
                    .and_then(|offset| start.checked_add(offset))
                    .ok_or_else(|| {
                        CoordinateError::InvalidRecord("spectral image section overflows".into())
                    })?;
                interpolate(pixel_values, world_values, source as f64).map(|world| world * factor)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let increment = (values[count - 1] - values[0]) / (count - 1).max(1) as f64;
        let Some(Value::Record(table)) = record.get_mut("tabular") else {
            unreachable!()
        };
        table.upsert(
            "crval",
            Value::Array(ArrayValue::from_f64_vec(vec![values[0]])),
        );
        table.upsert(
            "cdelt",
            Value::Array(ArrayValue::from_f64_vec(vec![increment])),
        );
        table.upsert("crpix", Value::Array(ArrayValue::from_f64_vec(vec![0.0])));
        table.upsert(
            "pixelvalues",
            Value::Array(ArrayValue::from_f64_vec(
                (0..count).map(|i| i as f64).collect(),
            )),
        );
        table.upsert(
            "worldvalues",
            Value::Array(ArrayValue::from_f64_vec(values)),
        );
        Ok(true)
    }
}
