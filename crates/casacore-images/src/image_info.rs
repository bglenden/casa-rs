// SPDX-License-Identifier: LGPL-3.0-or-later
//! Image metadata corresponding to C++ `casacore::ImageInfo`.

use std::fmt;
use std::str::FromStr;

use casacore_types::{RecordValue, ScalarValue, Value};

use crate::beam::{GaussianBeam, ImageBeamSet};
use crate::error::ImageError;

/// Classification of the physical quantity stored in image pixels.
///
/// This mirrors `ImageInfo::ImageTypes` in casacore C++.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ImageType {
    Undefined,
    Intensity,
    Beam,
    ColumnDensity,
    DepolarizationRatio,
    KineticTemperature,
    MagneticField,
    OpticalDepth,
    RotationMeasure,
    RotationalTemperature,
    SpectralIndex,
    Velocity,
    VelocityDispersion,
}

impl ImageType {
    /// Returns the FITS numeric image type when casacore defines one.
    pub fn to_fits_value(self) -> Option<i32> {
        match self {
            Self::Beam => Some(0),
            Self::SpectralIndex => Some(8),
            Self::OpticalDepth => Some(9),
            _ => None,
        }
    }

    /// Maps a FITS image-type code to the corresponding image type.
    pub fn from_fits_value(value: i32) -> Self {
        match value {
            0 => Self::Beam,
            8 => Self::SpectralIndex,
            9 => Self::OpticalDepth,
            _ => Self::Undefined,
        }
    }

    fn miriad_alias(input: &str) -> Option<Self> {
        match input.to_ascii_uppercase().as_str() {
            "COLUMN_DENSITY" => Some(Self::ColumnDensity),
            "DEPOLARIZATION_RATIO" => Some(Self::DepolarizationRatio),
            "KINETIC_TEMPERATURE" => Some(Self::KineticTemperature),
            "MAGNETIC_FIELD" => Some(Self::MagneticField),
            "OPTICAL_DEPTH" => Some(Self::OpticalDepth),
            "ROTATION_MEASURE" => Some(Self::RotationMeasure),
            "ROTATIONAL_TEMPERATURE" => Some(Self::RotationalTemperature),
            "SPECTRAL_INDEX" => Some(Self::SpectralIndex),
            "VELOCITY_DISPERSION" => Some(Self::VelocityDispersion),
            "VELOCITY" => Some(Self::Velocity),
            "INTENSITY" => Some(Self::Intensity),
            "BEAM" => Some(Self::Beam),
            _ => None,
        }
    }
}

impl fmt::Display for ImageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            Self::Undefined => "Undefined",
            Self::Intensity => "Intensity",
            Self::Beam => "Beam",
            Self::ColumnDensity => "Column Density",
            Self::DepolarizationRatio => "Depolarization Ratio",
            Self::KineticTemperature => "Kinetic Temperature",
            Self::MagneticField => "Magnetic Field",
            Self::OpticalDepth => "Optical Depth",
            Self::RotationMeasure => "Rotation Measure",
            Self::RotationalTemperature => "Rotational Temperature",
            Self::SpectralIndex => "Spectral Index",
            Self::Velocity => "Velocity",
            Self::VelocityDispersion => "Velocity Dispersion",
        };
        write!(f, "{text}")
    }
}

impl FromStr for ImageType {
    type Err = ImageError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let normalized = s.trim();
        if normalized.is_empty() {
            return Ok(Self::Undefined);
        }
        for image_type in [
            Self::Undefined,
            Self::Intensity,
            Self::Beam,
            Self::ColumnDensity,
            Self::DepolarizationRatio,
            Self::KineticTemperature,
            Self::MagneticField,
            Self::OpticalDepth,
            Self::RotationMeasure,
            Self::RotationalTemperature,
            Self::SpectralIndex,
            Self::Velocity,
            Self::VelocityDispersion,
        ] {
            if image_type.to_string().eq_ignore_ascii_case(normalized) {
                return Ok(image_type);
            }
        }
        if let Some(alias) = Self::miriad_alias(normalized) {
            return Ok(alias);
        }
        Err(ImageError::InvalidMetadata(format!(
            "unknown image type: {normalized}"
        )))
    }
}

/// Image metadata bundle corresponding to C++ `casacore::ImageInfo`.
#[derive(Debug, Clone, PartialEq)]
pub struct ImageInfo {
    /// Single-beam or per-plane beam metadata.
    pub beam_set: ImageBeamSet,
    /// The physical quantity classification.
    pub image_type: ImageType,
    /// The astronomical object name.
    pub object_name: String,
}

impl Default for ImageInfo {
    fn default() -> Self {
        Self {
            beam_set: ImageBeamSet::default(),
            image_type: ImageType::Undefined,
            object_name: String::new(),
        }
    }
}

impl ImageInfo {
    /// Returns the default image type used by casacore.
    pub fn default_image_type() -> ImageType {
        ImageType::Undefined
    }

    /// Returns the default object name used by casacore.
    pub fn default_object_name() -> String {
        String::new()
    }

    /// Returns the default restoring beam used by casacore.
    pub fn default_restoring_beam() -> GaussianBeam {
        GaussianBeam::default()
    }

    /// Serializes the image info to a casacore-compatible keyword record.
    pub fn to_record(&self) -> RecordValue {
        let mut rec = RecordValue::default();
        if self.beam_set.is_multi() {
            rec.upsert("perplanebeams", Value::Record(self.beam_set.to_record()));
        } else if let Some(beam) = self.beam_set.single_beam() {
            if !beam.is_null() {
                rec.upsert("restoringbeam", Value::Record(beam.to_record()));
            }
        }
        rec.upsert(
            "imagetype",
            Value::Scalar(ScalarValue::String(self.image_type.to_string())),
        );
        rec.upsert(
            "objectname",
            Value::Scalar(ScalarValue::String(self.object_name.clone())),
        );
        rec
    }

    /// Deserializes image info from a casacore-compatible keyword record.
    pub fn from_record(rec: &RecordValue) -> Result<Self, ImageError> {
        let beam_set = if let Some(Value::Record(beam_rec)) = rec.get("perplanebeams") {
            ImageBeamSet::from_record(beam_rec)?
        } else if let Some(Value::Record(beam_rec)) = rec.get("restoringbeam") {
            ImageBeamSet::new(GaussianBeam::from_record(beam_rec)?)
        } else {
            ImageBeamSet::default()
        };

        let image_type = match rec.get("imagetype") {
            Some(Value::Scalar(ScalarValue::String(s))) => s.parse()?,
            _ => ImageType::Undefined,
        };

        let object_name = match rec.get("objectname") {
            Some(Value::Scalar(ScalarValue::String(s))) => s.clone(),
            _ => String::new(),
        };

        Ok(Self {
            beam_set,
            image_type,
            object_name,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn image_type_display() {
        assert_eq!(ImageType::Intensity.to_string(), "Intensity");
        assert_eq!(ImageType::ColumnDensity.to_string(), "Column Density");
        assert_eq!(
            ImageType::VelocityDispersion.to_string(),
            "Velocity Dispersion"
        );
    }

    #[test]
    fn image_type_from_str_accepts_casacore_and_miriad_names() {
        assert_eq!(
            "Intensity".parse::<ImageType>().unwrap(),
            ImageType::Intensity
        );
        assert_eq!(
            "Column Density".parse::<ImageType>().unwrap(),
            ImageType::ColumnDensity
        );
        assert_eq!(
            "COLUMN_DENSITY".parse::<ImageType>().unwrap(),
            ImageType::ColumnDensity
        );
        assert_eq!(
            "velocity_dispersion".parse::<ImageType>().unwrap(),
            ImageType::VelocityDispersion
        );
        assert!("Garbage".parse::<ImageType>().is_err());
    }

    #[test]
    fn fits_image_type_mapping() {
        assert_eq!(ImageType::from_fits_value(0), ImageType::Beam);
        assert_eq!(ImageType::from_fits_value(8), ImageType::SpectralIndex);
        assert_eq!(ImageType::from_fits_value(9), ImageType::OpticalDepth);
        assert_eq!(ImageType::Velocity.to_fits_value(), None);
    }

    #[test]
    fn image_info_single_beam_round_trip() {
        let beam = GaussianBeam::new(1e-4, 5e-5, 0.3);
        let info = ImageInfo {
            beam_set: ImageBeamSet::new(beam),
            image_type: ImageType::Intensity,
            object_name: "M31".into(),
        };
        let rec = info.to_record();
        assert!(rec.get("restoringbeam").is_some());
        let back = ImageInfo::from_record(&rec).unwrap();
        assert_eq!(info, back);
    }

    #[test]
    fn image_info_multi_beam_round_trip() {
        let beams = ImageBeamSet::from_grid(vec![
            vec![
                GaussianBeam::new(1.0, 0.5, 0.0),
                GaussianBeam::new(1.1, 0.5, 0.0),
            ],
            vec![
                GaussianBeam::new(1.2, 0.5, 0.0),
                GaussianBeam::new(1.3, 0.5, 0.0),
            ],
        ]);
        let info = ImageInfo {
            beam_set: beams.clone(),
            image_type: ImageType::Velocity,
            object_name: "Cube".into(),
        };
        let rec = info.to_record();
        assert!(rec.get("perplanebeams").is_some());
        let back = ImageInfo::from_record(&rec).unwrap();
        assert_eq!(back.beam_set, beams);
        assert_eq!(back.image_type, ImageType::Velocity);
    }
}
