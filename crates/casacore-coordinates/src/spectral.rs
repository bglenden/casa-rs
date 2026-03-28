// SPDX-License-Identifier: LGPL-3.0-or-later
//! One-axis spectral (frequency/velocity) coordinate.
//!
//! [`SpectralCoordinate`] implements a one-axis spectral coordinate in a
//! specified reference frame. It supports both the simple linear WCS form and
//! the tabulated lookup-table form used by C++ `SpectralCoordinate`.

use std::str::FromStr;

use casacore_types::measures::direction_to_record;
use casacore_types::measures::epoch_to_record;
use casacore_types::measures::frequency::{FrequencyRef, MFrequency};
use casacore_types::measures::position_to_record;
use casacore_types::measures::{
    MeasFrame, direction_from_record, epoch_from_record, position_from_record,
};
use casacore_types::{ArrayValue, RecordValue, ScalarValue, Value};

use crate::coordinate::{Coordinate, CoordinateType};
use crate::error::CoordinateError;
use crate::record_utils::{
    get_optional_i32, get_optional_string, get_optional_vec_f64, get_optional_vec_string,
    get_required_f64, get_required_vec_f64,
};

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
    /// Optional CASA conversion-layer state for world-coordinate display.
    conversion: Option<SpectralConversion>,
    /// The rest frequency in Hz (used for velocity conversions).
    rest_frequency: f64,
    /// Pixel-to-world mapping state.
    mapping: SpectralMapping,
    /// The world axis name.
    name: String,
    /// The world axis unit string.
    unit: String,
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
            conversion: None,
            rest_frequency,
            mapping: SpectralMapping::Linear {
                crval,
                cdelt,
                crpix,
            },
            name: "Frequency".into(),
            unit: "Hz".into(),
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
            conversion: None,
            rest_frequency,
            mapping: SpectralMapping::Tabular {
                pixel_values,
                world_values,
                crval,
                cdelt,
                crpix,
            },
            name: "Frequency".into(),
            unit: "Hz".into(),
        })
    }

    /// Sets the world axis unit string. Returns `self` for chaining.
    pub fn with_unit(mut self, unit: impl Into<String>) -> Self {
        self.unit = unit.into();
        self
    }

    /// Sets the CASA conversion-layer frame used for displayed world coordinates.
    pub fn with_conversion(mut self, frequency_ref: FrequencyRef, frame: MeasFrame) -> Self {
        self.conversion = Some(SpectralConversion {
            frequency_ref,
            frame,
        });
        self
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

    /// Returns the CASA conversion-layer frequency frame, if configured.
    pub fn conversion_frequency_ref(&self) -> Option<FrequencyRef> {
        self.conversion
            .as_ref()
            .map(|conversion| conversion.frequency_ref)
    }

    /// Returns the world-coordinate frame presented by pixel/world conversion.
    pub fn world_frequency_ref(&self) -> FrequencyRef {
        self.conversion_frequency_ref()
            .unwrap_or(self.frequency_ref)
    }

    /// Returns the rest frequency in Hz.
    pub fn rest_frequency(&self) -> f64 {
        self.rest_frequency
    }

    /// Reconstructs a spectral coordinate from a serialized record.
    pub fn from_record(rec: &RecordValue) -> Result<Self, CoordinateError> {
        let frequency_ref = if let Some(name) =
            get_optional_string(rec, "frequency_ref").or_else(|| get_optional_string(rec, "system"))
        {
            FrequencyRef::from_str(&name).map_err(|err| {
                CoordinateError::InvalidRecord(format!("invalid spectral frequency_ref: {err}"))
            })?
        } else if let Some(code) = get_optional_i32(rec, "frequency_ref") {
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

        let crval = get_required_vec_f64(parameter_record, "crval")?;
        let cdelt = get_required_vec_f64(parameter_record, "cdelt")?;
        let crpix = get_required_vec_f64(parameter_record, "crpix")?;
        if crval.len() != 1 || cdelt.len() != 1 || crpix.len() != 1 {
            return Err(CoordinateError::InvalidRecord(
                "spectral coordinate expects single-valued crval/cdelt/crpix".into(),
            ));
        }

        let rest_frequency = get_required_f64(rec, "restfreq")?;
        let mut coord = if let Some((pixel_values, world_values)) = lookup_tables {
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
        if let Some(unit) = get_optional_string(rec, "unit").or_else(|| {
            get_optional_vec_string(parameter_record, "units")
                .and_then(|units| units.into_iter().next())
        }) {
            coord = coord.with_unit(unit);
        }
        if let Some(conversion) = parse_conversion(rec) {
            coord = coord.with_conversion(conversion.frequency_ref, conversion.frame);
        }
        Ok(coord)
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

    fn to_record(&self) -> RecordValue {
        let mut rec = RecordValue::default();

        rec.upsert(
            "coordinate_type",
            Value::Scalar(ScalarValue::String("Spectral".into())),
        );
        rec.upsert(
            "frequency_ref",
            Value::Scalar(ScalarValue::String(self.frequency_ref.as_str().into())),
        );
        rec.upsert(
            "restfreq",
            Value::Scalar(ScalarValue::Float64(self.rest_frequency)),
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
                rec.upsert(
                    "crval",
                    Value::Array(ArrayValue::from_f64_vec(vec![*crval])),
                );
                rec.upsert(
                    "cdelt",
                    Value::Array(ArrayValue::from_f64_vec(vec![*cdelt])),
                );
                rec.upsert(
                    "crpix",
                    Value::Array(ArrayValue::from_f64_vec(vec![*crpix])),
                );
            }
            SpectralMapping::Tabular {
                pixel_values,
                world_values,
                crval,
                cdelt,
                crpix,
            } => {
                let mut tabular = RecordValue::default();
                tabular.upsert(
                    "crval",
                    Value::Array(ArrayValue::from_f64_vec(vec![*crval])),
                );
                tabular.upsert(
                    "cdelt",
                    Value::Array(ArrayValue::from_f64_vec(vec![*cdelt])),
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
                    Value::Array(ArrayValue::from_string_vec(vec![self.unit.clone()])),
                );
                tabular.upsert(
                    "pixelvalues",
                    Value::Array(ArrayValue::from_f64_vec(pixel_values.clone())),
                );
                tabular.upsert(
                    "worldvalues",
                    Value::Array(ArrayValue::from_f64_vec(world_values.clone())),
                );
                rec.upsert("tabular", Value::Record(tabular));
            }
        }

        if let Some(conversion) = &self.conversion {
            if let (Some(direction), Some(position), Some(epoch)) = (
                conversion.frame.direction(),
                conversion.frame.position(),
                conversion.frame.epoch(),
            ) {
                let mut conversion_record = RecordValue::default();
                conversion_record.upsert(
                    "system",
                    Value::Scalar(ScalarValue::String(
                        conversion.frequency_ref.as_str().into(),
                    )),
                );
                conversion_record
                    .upsert("direction", Value::Record(direction_to_record(direction)));
                conversion_record.upsert("position", Value::Record(position_to_record(position)));
                conversion_record.upsert("epoch", Value::Record(epoch_to_record(epoch)));
                rec.upsert("conversion", Value::Record(conversion_record));
            }
        }

        rec
    }

    fn clone_box(&self) -> Box<dyn Coordinate> {
        Box::new(self.clone())
    }
}

impl SpectralCoordinate {
    fn convert_native_to_world(&self, native_hz: f64) -> Result<f64, CoordinateError> {
        let Some(conversion) = &self.conversion else {
            return Ok(native_hz);
        };
        MFrequency::new(native_hz, self.frequency_ref)
            .convert_to(conversion.frequency_ref, &conversion.frame)
            .map(|frequency| frequency.hz())
            .map_err(|err| {
                CoordinateError::ConversionFailed(format!(
                    "spectral frame conversion {} -> {} failed: {err}",
                    self.frequency_ref, conversion.frequency_ref
                ))
            })
    }

    fn convert_world_to_native(&self, world_hz: f64) -> Result<f64, CoordinateError> {
        let Some(conversion) = &self.conversion else {
            return Ok(world_hz);
        };
        MFrequency::new(world_hz, conversion.frequency_ref)
            .convert_to(self.frequency_ref, &conversion.frame)
            .map(|frequency| frequency.hz())
            .map_err(|err| {
                CoordinateError::ConversionFailed(format!(
                    "spectral frame conversion {} -> {} failed: {err}",
                    conversion.frequency_ref, self.frequency_ref
                ))
            })
    }
}

fn parse_conversion(rec: &RecordValue) -> Option<SpectralConversion> {
    let Value::Record(conversion) = rec.get("conversion")? else {
        return None;
    };
    let target = FrequencyRef::from_str(&get_optional_string(conversion, "system")?).ok()?;
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
    use casacore_types::measures::direction::{DirectionRef, MDirection};
    use casacore_types::measures::epoch::{EpochRef, MEpoch};
    use casacore_types::measures::position::MPosition;

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

    #[test]
    fn with_conversion_changes_world_frame() {
        let coord = SpectralCoordinate::new(FrequencyRef::LSRD, 1.15022e11, 6.25005e6, 0.0, 1.0)
            .with_conversion(FrequencyRef::LSRK, conversion_frame());
        assert_eq!(coord.frequency_ref(), FrequencyRef::LSRD);
        assert_eq!(coord.conversion_frequency_ref(), Some(FrequencyRef::LSRK));
        assert_eq!(coord.world_frequency_ref(), FrequencyRef::LSRK);
    }

    #[test]
    fn to_record_has_fields() {
        let coord = SpectralCoordinate::new(FrequencyRef::LSRK, 1e9, 1e6, 0.0, 1.42e9);
        let rec = coord.to_record();
        assert!(rec.get("frequency_ref").is_some());
        assert!(rec.get("restfreq").is_some());
        assert!(rec.get("crval").is_some());
    }

    #[test]
    fn clone_box_works() {
        let coord = SpectralCoordinate::new(FrequencyRef::TOPO, 1e9, 1e6, 0.0, 0.0);
        let boxed: Box<dyn Coordinate> = Box::new(coord);
        let cloned = boxed.clone_box();
        assert_eq!(cloned.coordinate_type(), CoordinateType::Spectral);
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
        assert_eq!(restored.increment(), vec![5.0e5]);
        let restored_world = restored.to_world(&[2.0]).unwrap();
        assert!((restored_world[0] - 1.41125e9).abs() < 1.0);
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
            casacore_types::RecordField::new(
                "crval",
                Value::Array(ArrayValue::from_f64_vec(vec![1.41e9])),
            ),
            casacore_types::RecordField::new(
                "crpix",
                Value::Array(ArrayValue::from_f64_vec(vec![0.0])),
            ),
            casacore_types::RecordField::new(
                "cdelt",
                Value::Array(ArrayValue::from_f64_vec(vec![5.0e5])),
            ),
            casacore_types::RecordField::new(
                "axes",
                Value::Array(ArrayValue::from_string_vec(vec!["Frequency".into()])),
            ),
            casacore_types::RecordField::new(
                "units",
                Value::Array(ArrayValue::from_string_vec(vec!["Hz".into()])),
            ),
            casacore_types::RecordField::new(
                "pixelvalues",
                Value::Array(ArrayValue::from_f64_vec(vec![0.0, 1.0, 3.0, 4.0])),
            ),
            casacore_types::RecordField::new(
                "worldvalues",
                Value::Array(ArrayValue::from_f64_vec(vec![
                    1.41e9, 1.4105e9, 1.412e9, 1.413e9,
                ])),
            ),
        ]);
        let record = RecordValue::new(vec![
            casacore_types::RecordField::new(
                "system",
                Value::Scalar(ScalarValue::String("TOPO".into())),
            ),
            casacore_types::RecordField::new(
                "restfreq",
                Value::Scalar(ScalarValue::Float64(1.42040575e9)),
            ),
            casacore_types::RecordField::new("tabular", Value::Record(tabular)),
        ]);

        let coord = SpectralCoordinate::from_record(&record).unwrap();
        assert_eq!(coord.frequency_ref(), FrequencyRef::TOPO);
        assert_eq!(coord.axis_names(), vec!["Frequency"]);
        assert_eq!(coord.axis_units(), vec!["Hz"]);
        assert_eq!(coord.reference_value(), vec![1.41e9]);
        assert_eq!(coord.reference_pixel(), vec![0.0]);
        assert_eq!(coord.increment(), vec![5.0e5]);
        let world = coord.to_world(&[2.0]).unwrap();
        assert!((world[0] - 1.41125e9).abs() < 1.0);
        let pixel = coord.to_pixel(&[1.41125e9]).unwrap();
        assert!((pixel[0] - 2.0).abs() < 1e-10);
    }
}
