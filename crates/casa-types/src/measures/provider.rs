// SPDX-License-Identifier: LGPL-3.0-or-later
//! Pure runtime-data contract for measure conversions.

use std::fmt::Debug;

/// Interpolated Earth-orientation values at one UTC epoch.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EopValues {
    /// UT1 minus UTC, in seconds.
    pub dut1_seconds: f64,
    /// Polar-motion X component, in arcseconds.
    pub x_arcsec: f64,
    /// Polar-motion Y component, in arcseconds.
    pub y_arcsec: f64,
    /// Celestial-pole dX offset, in milliarcseconds.
    pub dx_mas: f64,
    /// Celestial-pole dY offset, in milliarcseconds.
    pub dy_mas: f64,
    /// Whether either interpolation endpoint is predicted.
    pub is_predicted: bool,
}

/// Observatory coordinates returned by a measures-data provider.
#[derive(Debug, Clone, PartialEq)]
pub enum ObservatoryPosition {
    /// Geocentric ITRF coordinates, in metres.
    Itrf { x_m: f64, y_m: f64, z_m: f64 },
    /// WGS84 geodetic coordinates.
    Wgs84 {
        longitude_rad: f64,
        latitude_rad: f64,
        height_m: f64,
    },
}

/// Fixed named-source direction returned by a measures-data provider.
#[derive(Debug, Clone, PartialEq)]
pub struct NamedSourceDirection {
    /// Persisted casacore direction-reference name.
    pub reference: String,
    /// Longitude in radians.
    pub longitude_rad: f64,
    /// Latitude in radians.
    pub latitude_rad: f64,
}

/// Pure, immutable input-data boundary used by measure algorithms.
///
/// Implementations may load files or cache data, but `casa-types` depends only
/// on these scientific values and never performs discovery or I/O itself.
pub trait MeasuresProvider: Debug + Send + Sync {
    /// Interpolate EOP values at a UTC MJD.
    fn eop_values(&self, _utc_mjd: f64) -> Result<Option<EopValues>, String> {
        Ok(None)
    }

    /// Return TAI minus UTC in seconds at a UTC MJD.
    fn tai_minus_utc_seconds(&self, _utc_mjd: f64) -> Result<f64, String> {
        Err("TAI-UTC data is unavailable from this measures provider".to_string())
    }

    /// Convert a TAI MJD to UTC MJD.
    fn utc_from_tai_mjd(&self, _tai_mjd: f64) -> Result<f64, String> {
        Err("TAI-UTC data is unavailable from this measures provider".to_string())
    }

    /// Return IGRF coefficients and maximum spherical-harmonic degree.
    fn igrf_coefficients(&self, _decimal_year: f64) -> Result<(Vec<f64>, usize), String> {
        Err("IGRF data is unavailable from this measures provider".to_string())
    }

    /// Look up an observatory by case-insensitive name.
    fn observatory(&self, _name: &str) -> Result<Option<ObservatoryPosition>, String> {
        Ok(None)
    }

    /// Look up a fixed source by case-insensitive name.
    fn source(&self, _name: &str) -> Result<Option<NamedSourceDirection>, String> {
        Ok(None)
    }

    /// Look up a rest-line frequency by case-insensitive name.
    fn spectral_line_hz(&self, _name: &str) -> Result<Option<f64>, String> {
        Ok(None)
    }
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct TestMeasuresProvider;

#[cfg(test)]
impl MeasuresProvider for TestMeasuresProvider {
    fn eop_values(&self, _utc_mjd: f64) -> Result<Option<EopValues>, String> {
        Ok(Some(EopValues {
            dut1_seconds: 0.0,
            x_arcsec: 0.0,
            y_arcsec: 0.0,
            dx_mas: 0.0,
            dy_mas: 0.0,
            is_predicted: false,
        }))
    }

    fn tai_minus_utc_seconds(&self, _utc_mjd: f64) -> Result<f64, String> {
        Ok(32.0)
    }

    fn utc_from_tai_mjd(&self, tai_mjd: f64) -> Result<f64, String> {
        Ok(tai_mjd - 32.0 / 86_400.0)
    }

    fn igrf_coefficients(&self, decimal_year: f64) -> Result<(Vec<f64>, usize), String> {
        if !(1900.0..=2025.999).contains(&decimal_year) {
            return Err("test IGRF date is outside 1900 through 2025".to_string());
        }
        let mut coefficients = vec![0.0; 13 * 15];
        coefficients[0] = -29_440.0;
        coefficients[1] = -1_500.0;
        coefficients[2] = 4_650.0;
        Ok((coefficients, 13))
    }

    fn observatory(&self, name: &str) -> Result<Option<ObservatoryPosition>, String> {
        Ok(match name.to_ascii_uppercase().as_str() {
            "ALMA" => Some(ObservatoryPosition::Wgs84 {
                longitude_rad: -67.754_929_f64.to_radians(),
                latitude_rad: -23.022_886_f64.to_radians(),
                height_m: 5056.8,
            }),
            "VLA" => Some(ObservatoryPosition::Itrf {
                x_m: -1_601_185.0,
                y_m: -5_041_977.0,
                z_m: 3_554_875.0,
            }),
            "WSRT" => Some(ObservatoryPosition::Itrf {
                x_m: 3_826_577.0,
                y_m: 461_022.0,
                z_m: 5_064_892.0,
            }),
            _ => None,
        })
    }

    fn source(&self, name: &str) -> Result<Option<NamedSourceDirection>, String> {
        Ok(match name.to_ascii_uppercase().as_str() {
            "CASA" => Some(NamedSourceDirection {
                reference: "J2000".to_string(),
                longitude_rad: 6.123_487_680_622_104,
                latitude_rad: 1.026_515_399_560_464_8,
            }),
            "0002-478" => Some(NamedSourceDirection {
                reference: "ICRS".to_string(),
                longitude_rad: 0.020_046_3,
                latitude_rad: -0.830_872,
            }),
            _ => None,
        })
    }

    fn spectral_line_hz(&self, name: &str) -> Result<Option<f64>, String> {
        Ok(name.eq_ignore_ascii_case("HI").then_some(1.420_405_752e9))
    }
}

#[cfg(test)]
pub(crate) fn test_measures() -> std::sync::Arc<dyn MeasuresProvider> {
    std::sync::Arc::new(TestMeasuresProvider)
}
