// SPDX-License-Identifier: LGPL-3.0-or-later
//! Earth-magnetic measure support.
//!
//! This module provides:
//!
//! - [`EarthMagneticRef`] — Earth-magnetic reference frame types matching the
//!   C++ `MEarthMagnetic::Types` catalog.
//! - [`MEarthMagnetic`] — a magnetic-field vector in nanotesla tagged with a
//!   reference frame, equivalent to C++ `casa::MEarthMagnetic`.
//! - [`calculate_igrf`] — a reusable IGRF model helper corresponding to the
//!   C++ `EarthMagneticMachine` path used by TaQL `meas.igrf*` UDFs.
//!
//! The explicit vector conversions reuse the same frame-routing machinery as
//! [`super::direction::MDirection`]: the vector direction is converted between
//! reference frames while the field strength is preserved.

use std::fmt;
use std::str::FromStr;
use std::sync::OnceLock;

use casa_measures_data::bundled_igrf12_coefficients;
use time::Month;

use super::direction::{DirectionRef, MDirection};
use super::epoch::{EpochRef, MEpoch};
use super::error::MeasureError;
use super::frame::MeasFrame;
use super::position::MPosition;

/// Earth-magnetic reference frame types.
///
/// These correspond to the non-model frame values in C++ `MEarthMagnetic::Types`,
/// plus the special model code [`IGRF`](EarthMagneticRef::IGRF).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EarthMagneticRef {
    J2000,
    JMEAN,
    JTRUE,
    APP,
    B1950,
    GALACTIC,
    HADEC,
    AZEL,
    AZELSW,
    AZELGEO,
    AZELSWGEO,
    JNAT,
    ECLIPTIC,
    MECLIPTIC,
    TECLIPTIC,
    SUPERGAL,
    ITRF,
    TOPO,
    ICRS,
    /// International Geomagnetic Reference Field model identifier.
    IGRF,
}

impl EarthMagneticRef {
    /// All supported Earth-magnetic reference types.
    pub const ALL: [Self; 20] = [
        Self::J2000,
        Self::JMEAN,
        Self::JTRUE,
        Self::APP,
        Self::B1950,
        Self::GALACTIC,
        Self::HADEC,
        Self::AZEL,
        Self::AZELSW,
        Self::AZELGEO,
        Self::AZELSWGEO,
        Self::JNAT,
        Self::ECLIPTIC,
        Self::MECLIPTIC,
        Self::TECLIPTIC,
        Self::SUPERGAL,
        Self::ITRF,
        Self::TOPO,
        Self::ICRS,
        Self::IGRF,
    ];

    /// Returns the C++ casacore integer code for this reference type.
    pub fn casacore_code(self) -> i32 {
        match self {
            Self::J2000 => 0,
            Self::JMEAN => 1,
            Self::JTRUE => 2,
            Self::APP => 3,
            Self::B1950 => 4,
            Self::GALACTIC => 8,
            Self::HADEC => 9,
            Self::AZEL => 10,
            Self::AZELSW => 11,
            Self::AZELGEO => 12,
            Self::AZELSWGEO => 13,
            Self::JNAT => 14,
            Self::ECLIPTIC => 15,
            Self::MECLIPTIC => 16,
            Self::TECLIPTIC => 17,
            Self::SUPERGAL => 18,
            Self::ITRF => 19,
            Self::TOPO => 20,
            Self::ICRS => 21,
            Self::IGRF => 32,
        }
    }

    /// Converts a C++ casacore integer code to the corresponding reference type.
    pub fn from_casacore_code(code: i32) -> Option<Self> {
        match code {
            0 => Some(Self::J2000),
            1 => Some(Self::JMEAN),
            2 => Some(Self::JTRUE),
            3 => Some(Self::APP),
            4 => Some(Self::B1950),
            8 => Some(Self::GALACTIC),
            9 => Some(Self::HADEC),
            10 => Some(Self::AZEL),
            11 => Some(Self::AZELSW),
            12 => Some(Self::AZELGEO),
            13 => Some(Self::AZELSWGEO),
            14 => Some(Self::JNAT),
            15 => Some(Self::ECLIPTIC),
            16 => Some(Self::MECLIPTIC),
            17 => Some(Self::TECLIPTIC),
            18 => Some(Self::SUPERGAL),
            19 => Some(Self::ITRF),
            20 => Some(Self::TOPO),
            21 => Some(Self::ICRS),
            32 => Some(Self::IGRF),
            _ => None,
        }
    }

    /// Returns the canonical string name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::J2000 => "J2000",
            Self::JMEAN => "JMEAN",
            Self::JTRUE => "JTRUE",
            Self::APP => "APP",
            Self::B1950 => "B1950",
            Self::GALACTIC => "GALACTIC",
            Self::HADEC => "HADEC",
            Self::AZEL => "AZEL",
            Self::AZELSW => "AZELSW",
            Self::AZELGEO => "AZELGEO",
            Self::AZELSWGEO => "AZELSWGEO",
            Self::JNAT => "JNAT",
            Self::ECLIPTIC => "ECLIPTIC",
            Self::MECLIPTIC => "MECLIPTIC",
            Self::TECLIPTIC => "TECLIPTIC",
            Self::SUPERGAL => "SUPERGAL",
            Self::ITRF => "ITRF",
            Self::TOPO => "TOPO",
            Self::ICRS => "ICRS",
            Self::IGRF => "IGRF",
        }
    }

    fn to_direction_ref(self) -> Option<DirectionRef> {
        Some(match self {
            Self::J2000 => DirectionRef::J2000,
            Self::JMEAN => DirectionRef::JMEAN,
            Self::JTRUE => DirectionRef::JTRUE,
            Self::APP => DirectionRef::APP,
            Self::B1950 => DirectionRef::B1950,
            Self::GALACTIC => DirectionRef::GALACTIC,
            Self::HADEC => DirectionRef::HADEC,
            Self::AZEL => DirectionRef::AZEL,
            Self::AZELSW => DirectionRef::AZELSW,
            Self::AZELGEO => DirectionRef::AZELGEO,
            Self::AZELSWGEO => DirectionRef::AZELSWGEO,
            Self::JNAT => DirectionRef::JNAT,
            Self::ECLIPTIC => DirectionRef::ECLIPTIC,
            Self::MECLIPTIC => DirectionRef::MECLIPTIC,
            Self::TECLIPTIC => DirectionRef::TECLIPTIC,
            Self::SUPERGAL => DirectionRef::SUPERGAL,
            Self::ITRF => DirectionRef::ITRF,
            Self::TOPO => DirectionRef::TOPO,
            Self::ICRS => DirectionRef::ICRS,
            Self::IGRF => return None,
        })
    }
}

impl FromStr for EarthMagneticRef {
    type Err = MeasureError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "J2000" => Ok(Self::J2000),
            "JMEAN" => Ok(Self::JMEAN),
            "JTRUE" => Ok(Self::JTRUE),
            "APP" => Ok(Self::APP),
            "B1950" => Ok(Self::B1950),
            "GALACTIC" | "GAL" => Ok(Self::GALACTIC),
            "HADEC" => Ok(Self::HADEC),
            "AZEL" | "AZELNE" => Ok(Self::AZEL),
            "AZELSW" => Ok(Self::AZELSW),
            "AZELGEO" | "AZELNEGEO" => Ok(Self::AZELGEO),
            "AZELSWGEO" => Ok(Self::AZELSWGEO),
            "JNAT" => Ok(Self::JNAT),
            "ECLIPTIC" | "ECL" => Ok(Self::ECLIPTIC),
            "MECLIPTIC" => Ok(Self::MECLIPTIC),
            "TECLIPTIC" => Ok(Self::TECLIPTIC),
            "SUPERGAL" | "SGAL" | "SUPERGALACTIC" => Ok(Self::SUPERGAL),
            "ITRF" => Ok(Self::ITRF),
            "TOPO" => Ok(Self::TOPO),
            "ICRS" => Ok(Self::ICRS),
            "IGRF" => Ok(Self::IGRF),
            _ => Err(MeasureError::UnknownRefType {
                input: s.to_string(),
            }),
        }
    }
}

impl fmt::Display for EarthMagneticRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// An Earth-magnetic field vector in nanotesla.
///
/// The stored vector components are expressed in the coordinate system named by
/// [`refer`](MEarthMagnetic::refer). The vector length is preserved across
/// frame conversions.
#[derive(Debug, Clone)]
pub struct MEarthMagnetic {
    vector_nt: [f64; 3],
    refer: EarthMagneticRef,
}

impl MEarthMagnetic {
    /// Creates a field vector from Cartesian components in nanotesla.
    pub fn from_xyz_nt(x_nt: f64, y_nt: f64, z_nt: f64, refer: EarthMagneticRef) -> Self {
        Self {
            vector_nt: [x_nt, y_nt, z_nt],
            refer,
        }
    }

    /// Creates a field vector from spherical angles and total field strength.
    pub fn from_angles(
        longitude_rad: f64,
        latitude_rad: f64,
        length_nt: f64,
        refer: EarthMagneticRef,
    ) -> Self {
        let unit = sofars::vm::s2c(longitude_rad, latitude_rad);
        Self::from_xyz_nt(
            unit[0] * length_nt,
            unit[1] * length_nt,
            unit[2] * length_nt,
            refer,
        )
    }

    /// Returns the raw vector components in nanotesla.
    pub fn xyz_nt(&self) -> [f64; 3] {
        self.vector_nt
    }

    /// Returns the reference frame.
    pub fn refer(&self) -> EarthMagneticRef {
        self.refer
    }

    /// Returns the total field strength in nanotesla.
    pub fn length_nt(&self) -> f64 {
        let [x, y, z] = self.vector_nt;
        (x * x + y * y + z * z).sqrt()
    }

    /// Returns the field direction as `(longitude_rad, latitude_rad)`.
    pub fn angles_rad(&self) -> (f64, f64) {
        let length = self.length_nt();
        if length == 0.0 {
            return (0.0, 0.0);
        }
        let unit = [
            self.vector_nt[0] / length,
            self.vector_nt[1] / length,
            self.vector_nt[2] / length,
        ];
        let (lon, lat) = sofars::vm::c2s(&unit);
        (sofars::vm::anp(lon), lat)
    }

    /// Converts this field vector to another reference frame.
    ///
    /// This mirrors C++ `MEarthMagnetic::Convert` for explicit field vectors:
    /// the vector direction is transformed using the corresponding direction
    /// frame route and the field magnitude is preserved.
    pub fn convert_to(
        &self,
        target: EarthMagneticRef,
        frame: &MeasFrame,
    ) -> Result<Self, MeasureError> {
        if self.refer == target {
            return Ok(self.clone());
        }
        let src_dir =
            self.refer
                .to_direction_ref()
                .ok_or_else(|| MeasureError::NotYetImplemented {
                    route: format!("MEarthMagnetic source reference {}", self.refer),
                })?;
        let dst_dir = target
            .to_direction_ref()
            .ok_or_else(|| MeasureError::NotYetImplemented {
                route: format!("MEarthMagnetic target reference {target}"),
            })?;

        let length = self.length_nt();
        if length == 0.0 {
            return Ok(Self::from_xyz_nt(0.0, 0.0, 0.0, target));
        }

        let dir = MDirection::from_cosines(self.vector_nt, src_dir);
        let converted = dir.convert_to(dst_dir, frame)?;
        let cosines = converted.cosines();
        Ok(Self::from_xyz_nt(
            cosines[0] * length,
            cosines[1] * length,
            cosines[2] * length,
            target,
        ))
    }
}

/// Result of an IGRF model evaluation.
///
/// This corresponds to the information C++ `EarthMagneticMachine` exposes to
/// TaQL `meas.igrf*` helpers: the field vector itself, the line-of-sight field,
/// and the longitude of the calculation point.
#[derive(Debug, Clone)]
pub struct IgrfSample {
    /// Field vector in the ITRF frame.
    pub field: MEarthMagnetic,
    /// Line-of-sight field component in nanotesla.
    pub los_field_nt: f64,
    /// Longitude of the calculation point in radians.
    pub longitude_rad: f64,
    /// Position at which the field was evaluated.
    pub position: MPosition,
}

/// Calculate the IGRF field for a height and direction.
///
/// The frame must contain:
/// - an epoch, used to select the IGRF coefficients, and
/// - an observer position, used together with `direction` to locate the
///   sub-ionospheric calculation point.
///
/// The returned field vector is always in the ITRF frame. Convert it to other
/// frames with [`MEarthMagnetic::convert_to`] when needed.
pub fn calculate_igrf(
    height_m: f64,
    direction: &MDirection,
    frame: &MeasFrame,
) -> Result<IgrfSample, MeasureError> {
    if height_m < 0.0 {
        return Err(MeasureError::ModelError {
            model: "IGRF",
            reason: "height must be non-negative".to_string(),
        });
    }

    let observer = frame.position().ok_or(MeasureError::MissingFrameData {
        what: "position (for IGRF)",
    })?;
    let epoch = frame.epoch().ok_or(MeasureError::MissingFrameData {
        what: "epoch (for IGRF)",
    })?;

    let dir_itrf = direction.convert_to(DirectionRef::ITRF, frame)?;
    let calculation_pos = subpoint_position(observer, dir_itrf.cosines(), height_m);
    let (field_xyz_nt, longitude_rad) = igrf_field_xyz(&calculation_pos, epoch, frame)?;
    let los_field_nt = dot(field_xyz_nt, dir_itrf.cosines());

    Ok(IgrfSample {
        field: MEarthMagnetic::from_xyz_nt(
            field_xyz_nt[0],
            field_xyz_nt[1],
            field_xyz_nt[2],
            EarthMagneticRef::ITRF,
        ),
        los_field_nt,
        longitude_rad,
        position: calculation_pos,
    })
}

fn subpoint_position(observer: &MPosition, direction_itrf: [f64; 3], height_m: f64) -> MPosition {
    let obs_xyz = observer.as_itrf();
    let posl = norm(obs_xyz);
    let subl = height_m * (height_m + 2.0 * posl);
    let an = dot(obs_xyz, direction_itrf);
    let x = (an * an + subl).abs().sqrt();
    let x = (-an + x).abs().min((-an - x).abs());
    MPosition::new_itrf(
        obs_xyz[0] + x * direction_itrf[0],
        obs_xyz[1] + x * direction_itrf[1],
        obs_xyz[2] + x * direction_itrf[2],
    )
}

fn igrf_field_xyz(
    position: &MPosition,
    epoch: &MEpoch,
    frame: &MeasFrame,
) -> Result<([f64; 3], f64), MeasureError> {
    let spherical = position.as_spherical();
    let date = epoch_to_igrf_date(epoch, frame)?;
    let coeffs = igrf12_coefficients_for_date(date)?;
    Ok((
        earth_field_xyz_itrf(spherical, &coeffs, igrf12_data().nmax),
        spherical.0,
    ))
}

fn epoch_to_igrf_date(epoch: &MEpoch, frame: &MeasFrame) -> Result<time::Date, MeasureError> {
    let tdb = epoch.convert_to(EpochRef::TDB, frame)?;
    let (jd1, jd2) = tdb.value().as_jd_pair();
    let (year, month, day, _) =
        sofars::cal::jd2cal(jd1, jd2).map_err(|code| MeasureError::SofarsError { code })?;
    let month = Month::try_from(month as u8).map_err(|err| MeasureError::ModelError {
        model: "IGRF",
        reason: err.to_string(),
    })?;
    time::Date::from_calendar_date(year, month, day as u8).map_err(|err| MeasureError::ModelError {
        model: "IGRF",
        reason: err.to_string(),
    })
}

fn dot(a: [f64; 3], b: [f64; 3]) -> f64 {
    a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
}

fn norm(v: [f64; 3]) -> f64 {
    dot(v, v).sqrt()
}

struct Igrf12Data {
    years: Vec<f64>,
    coeffs_by_year: Vec<Vec<f64>>,
    secular_variation: Vec<f64>,
    nmax: usize,
}

fn igrf12_data() -> &'static Igrf12Data {
    static DATA: OnceLock<Igrf12Data> = OnceLock::new();
    DATA.get_or_init(|| parse_igrf12_coefficients(bundled_igrf12_coefficients()))
}

fn parse_igrf12_coefficients(input: &str) -> Igrf12Data {
    let mut years = Vec::new();
    let mut coeffs_by_year: Vec<Vec<f64>> = Vec::new();
    let mut secular_variation = vec![0.0];
    let mut nmax = 0usize;

    for line in input.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if trimmed.starts_with("g/h ") {
            let header: Vec<&str> = trimmed.split_whitespace().collect();
            years = header[3..header.len() - 1]
                .iter()
                .map(|value| {
                    value
                        .parse::<f64>()
                        .expect("bundled igrf12coeffs.txt year header must parse")
                })
                .collect();
            coeffs_by_year = years.iter().map(|_| vec![0.0]).collect();
            continue;
        }

        let fields: Vec<&str> = trimmed.split_whitespace().collect();
        if fields.len() < 4 || fields[1].parse::<usize>().is_err() {
            continue;
        }

        let degree = fields[1]
            .parse::<usize>()
            .expect("bundled igrf12coeffs.txt degree must parse");
        nmax = nmax.max(degree);
        for (idx, coeffs) in coeffs_by_year.iter_mut().enumerate() {
            coeffs.push(
                fields[idx + 3]
                    .parse::<f64>()
                    .expect("bundled igrf12coeffs.txt coefficient must parse"),
            );
        }
        secular_variation.push(
            fields
                .last()
                .expect("bundled igrf12coeffs.txt SV column must exist")
                .parse::<f64>()
                .expect("bundled igrf12coeffs.txt SV coefficient must parse"),
        );
    }

    assert!(
        !years.is_empty()
            && coeffs_by_year
                .iter()
                .all(|c| c.len() == secular_variation.len()),
        "bundled igrf12coeffs.txt must contain consistent coefficients"
    );

    Igrf12Data {
        years,
        coeffs_by_year,
        secular_variation,
        nmax,
    }
}

fn igrf12_coefficients_for_date(date: time::Date) -> Result<Vec<f64>, MeasureError> {
    let data = igrf12_data();
    let date = decimal_day_of_year(date);
    let min_year = *data.years.first().expect("IGRF12 years must not be empty");
    let max_year = data.years.last().copied().unwrap_or(min_year) + 5.0;
    if date < min_year || date > max_year {
        return Err(MeasureError::ModelError {
            model: "IGRF",
            reason: format!("date must be between {min_year:.0}-01-01 and {max_year:.0}-12-31"),
        });
    }

    Ok(
        if date >= *data.years.last().expect("IGRF12 years must not be empty") {
            extrapolate_coefficients(
                date,
                *data.years.last().expect("IGRF12 years must not be empty"),
                &data.coeffs_by_year[data.coeffs_by_year.len() - 1],
                &data.secular_variation,
            )
        } else {
            let upper = data
                .years
                .iter()
                .position(|year| *year > date)
                .expect("date below last year must have an upper interval");
            interpolate_coefficients(
                date,
                data.years[upper - 1],
                &data.coeffs_by_year[upper - 1],
                data.years[upper],
                &data.coeffs_by_year[upper],
            )
        },
    )
}

fn decimal_day_of_year(date: time::Date) -> f64 {
    let day = date.day() as i32;
    let month = date.month() as i32;
    let year = date.year();

    const DAYS: [i32; 12] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    let leap_year = if (year % 4 == 0) && ((year % 100 != 0) || (year % 400 == 0)) {
        1
    } else {
        0
    };
    let mut day_in_year = DAYS[(month - 1) as usize] + day;
    if month > 2 {
        day_in_year += leap_year;
    }
    f64::from(year) + (f64::from(day_in_year) / (365.0 + f64::from(leap_year)))
}

fn extrapolate_coefficients(date: f64, base_year: f64, main: &[f64], sv: &[f64]) -> Vec<f64> {
    let factor = date - base_year;
    main.iter()
        .zip(sv.iter())
        .map(|(main_coeff, sv_coeff)| main_coeff + factor * sv_coeff)
        .collect()
}

fn interpolate_coefficients(
    date: f64,
    year1: f64,
    coeffs1: &[f64],
    year2: f64,
    coeffs2: &[f64],
) -> Vec<f64> {
    let factor = (date - year1) / (year2 - year1);
    coeffs1
        .iter()
        .zip(coeffs2.iter())
        .map(|(lhs, rhs)| lhs + factor * (rhs - lhs))
        .collect()
}

fn earth_field_xyz_itrf(spherical: (f64, f64, f64), gh: &[f64], nmax: usize) -> [f64; 3] {
    let (lon_rad, lat_rad, radius_m) = spherical;
    let slat = lat_rad.sin();
    let clat = lat_rad.cos();
    let slong = lon_rad.sin();
    let clong = lon_rad.cos();
    let ratio = 6_371_200.0 / radius_m;

    let npq = nmax * (nmax + 3) / 2;
    let mut p = vec![0.0; 119];
    let mut q = vec![0.0; 119];
    let mut cl = vec![0.0; 2 * 119];
    let mut sl = vec![0.0; 2 * 119];

    cl[0] = clong;
    sl[0] = slong;
    p[0] = 2.0 * slat;
    p[1] = 2.0 * clat;
    p[2] = 4.5 * slat * slat - 1.5;
    p[3] = 5.196_152_4 * clat * slat;
    q[0] = -clat;
    q[1] = slat;
    q[2] = -3.0 * clat * slat;
    q[3] = 1.732_050_8 * (slat * slat - clat * clat);

    let mut x = 0.0;
    let mut y = 0.0;
    let mut z = 0.0;
    let mut l = 0usize;
    let mut m = 0isize;
    let mut n = 0usize;
    let mut fn_ = 0.0;
    let mut rr = 0.0;

    for k in 0..npq {
        if (n as isize - m - 1) < 0 {
            m = -1;
            n += 1;
            rr = ratio.powi(n as i32 + 2);
            fn_ = n as f64;
        }
        let fm = (m + 1) as f64;
        if k >= 4 {
            if (m + 1) as usize == n {
                let one = (1.0 - 0.5 / fm).sqrt();
                let j = k - n - 1;
                p[k] = (1.0 + 1.0 / fm) * one * clat * p[j];
                q[k] = one * (clat * q[j] + slat / fm * p[j]);
                let mu = m as usize;
                sl[mu] = sl[mu - 1] * cl[0] + cl[mu - 1] * sl[0];
                cl[mu] = cl[mu - 1] * cl[0] - sl[mu - 1] * sl[0];
            } else {
                let one = ((fn_ * fn_) - (fm * fm)).sqrt();
                let two = ((((fn_ - 1.0) * (fn_ - 1.0)) - (fm * fm)).sqrt()) / one;
                let three = (2.0 * fn_ - 1.0) / one;
                let i = (k as isize - n as isize) as usize;
                let j = (k as isize - 2 * n as isize + 1) as usize;
                p[k] = (fn_ + 1.0) * (three * slat / fn_ * p[i] - two / (fn_ - 1.0) * p[j]);
                q[k] = three * (slat * q[i] - clat / fn_ * p[i]) - two * q[j];
            }
        }

        let one = gh[l + 1] * rr;
        if m == -1 {
            x += one * q[k];
            z -= one * p[k];
            l += 1;
        } else {
            let two = gh[l + 2] * rr;
            let mu = m as usize;
            let three = one * cl[mu] + two * sl[mu];
            x += three * q[k];
            z -= three * p[k];
            if clat > 0.0 {
                y += (one * sl[mu] - two * cl[mu]) * fm * p[k] / ((fn_ + 1.0) * clat);
            } else {
                y += (one * sl[mu] - two * cl[mu]) * q[k] * slat;
            }
            l += 2;
        }
        m += 1;
    }

    [
        x * slat * clong + z * clat * clong + y * slong,
        -x * slat * slong + z * clat * slong - y * clong,
        -x * clat + z * slat,
    ]
}

#[cfg(test)]
mod tests {
    use super::{
        EarthMagneticRef, MEarthMagnetic, calculate_igrf, decimal_day_of_year,
        igrf12_coefficients_for_date, parse_igrf12_coefficients,
    };
    use crate::measures::direction::{DirectionRef, MDirection};
    use crate::measures::error::MeasureError;
    use crate::measures::{EpochRef, MEpoch, MPosition, MeasFrame};
    use time::{Date, Month};

    fn test_frame() -> MeasFrame {
        MeasFrame::new()
            .with_epoch(MEpoch::from_mjd(51544.5, EpochRef::UTC))
            .with_position(MPosition::new_wgs84(-1.878_283_2, 0.595_370_3, 2124.0))
            .with_bundled_eop()
    }

    #[test]
    fn earthmag_ref_aliases_and_codes_roundtrip() {
        for refer in EarthMagneticRef::ALL {
            assert_eq!(
                EarthMagneticRef::from_casacore_code(refer.casacore_code()),
                Some(refer)
            );
            assert_eq!(refer.to_string(), refer.as_str());
        }

        assert_eq!(
            "gal".parse::<EarthMagneticRef>().unwrap(),
            EarthMagneticRef::GALACTIC
        );
        assert_eq!(
            "ecl".parse::<EarthMagneticRef>().unwrap(),
            EarthMagneticRef::ECLIPTIC
        );
        assert_eq!(
            "sgal".parse::<EarthMagneticRef>().unwrap(),
            EarthMagneticRef::SUPERGAL
        );
        assert_eq!(
            "azelne".parse::<EarthMagneticRef>().unwrap(),
            EarthMagneticRef::AZEL
        );
        assert_eq!(
            "azelnegeo".parse::<EarthMagneticRef>().unwrap(),
            EarthMagneticRef::AZELGEO
        );
        assert!(matches!(
            "bogus".parse::<EarthMagneticRef>(),
            Err(MeasureError::UnknownRefType { .. })
        ));
        assert_eq!(EarthMagneticRef::from_casacore_code(999), None);
    }

    #[test]
    fn earthmag_convert_preserves_strength() {
        let frame = test_frame();
        let field = MEarthMagnetic::from_xyz_nt(-8.0, -1.0, 5.0, EarthMagneticRef::J2000);
        let converted = field.convert_to(EarthMagneticRef::APP, &frame).unwrap();

        assert!((field.length_nt() - converted.length_nt()).abs() < 1e-12);
    }

    #[test]
    fn earthmag_from_angles_roundtrip_and_zero_cases() {
        let field = MEarthMagnetic::from_angles(0.3, -0.2, 5.0e4, EarthMagneticRef::J2000);
        let (lon, lat) = field.angles_rad();
        assert!((lon - 0.3).abs() < 1e-12);
        assert!((lat + 0.2).abs() < 1e-12);
        assert!((field.length_nt() - 5.0e4).abs() < 1e-9);

        let zero = MEarthMagnetic::from_xyz_nt(0.0, 0.0, 0.0, EarthMagneticRef::J2000);
        assert_eq!(zero.angles_rad(), (0.0, 0.0));
        let converted = zero
            .convert_to(EarthMagneticRef::APP, &test_frame())
            .unwrap();
        assert_eq!(converted.xyz_nt(), [0.0, 0.0, 0.0]);
    }

    #[test]
    fn earthmag_convert_rejects_igrf_references() {
        let frame = test_frame();
        let field = MEarthMagnetic::from_xyz_nt(-8.0, -1.0, 5.0, EarthMagneticRef::J2000);
        assert!(matches!(
            field.convert_to(EarthMagneticRef::IGRF, &frame),
            Err(MeasureError::NotYetImplemented { .. })
        ));

        let igrf = MEarthMagnetic::from_xyz_nt(-8.0, -1.0, 5.0, EarthMagneticRef::IGRF);
        assert!(matches!(
            igrf.convert_to(EarthMagneticRef::J2000, &frame),
            Err(MeasureError::NotYetImplemented { .. })
        ));
    }

    #[test]
    fn igrf_sample_is_finite_for_zenith() {
        let frame = test_frame();
        let direction =
            MDirection::from_angles(0.0, std::f64::consts::FRAC_PI_2, DirectionRef::AZEL);
        let sample = calculate_igrf(0.0, &direction, &frame).unwrap();
        let xyz = sample.field.xyz_nt();

        assert!(xyz.iter().all(|v| v.is_finite()));
        assert!(sample.los_field_nt.is_finite());
        assert!(sample.longitude_rad.is_finite());
    }

    #[test]
    fn igrf_validation_errors_cover_missing_frame_and_negative_height() {
        let direction =
            MDirection::from_angles(0.0, std::f64::consts::FRAC_PI_2, DirectionRef::AZEL);
        assert!(matches!(
            calculate_igrf(-1.0, &direction, &test_frame()),
            Err(MeasureError::ModelError { .. })
        ));
        assert!(matches!(
            calculate_igrf(0.0, &direction, &MeasFrame::new()),
            Err(MeasureError::MissingFrameData { .. })
        ));
        assert!(matches!(
            calculate_igrf(
                0.0,
                &direction,
                &MeasFrame::new().with_epoch(MEpoch::from_mjd(51544.5, EpochRef::UTC))
            ),
            Err(MeasureError::MissingFrameData { .. })
        ));
    }

    #[test]
    fn igrf_coefficients_and_date_helpers_cover_supported_ranges() {
        let leap = Date::from_calendar_date(2016, Month::March, 1).unwrap();
        let non_leap = Date::from_calendar_date(2015, Month::March, 1).unwrap();
        assert!(decimal_day_of_year(leap) > decimal_day_of_year(non_leap));

        let parsed = parse_igrf12_coefficients(casa_measures_data::bundled_igrf12_coefficients());
        assert!(!parsed.years.is_empty());
        assert!(parsed.nmax > 0);
        assert_eq!(parsed.coeffs_by_year.len(), parsed.years.len());

        let interpolated =
            igrf12_coefficients_for_date(Date::from_calendar_date(2012, Month::June, 30).unwrap())
                .unwrap();
        let extrapolated =
            igrf12_coefficients_for_date(Date::from_calendar_date(2019, Month::June, 30).unwrap())
                .unwrap();
        assert_eq!(interpolated.len(), extrapolated.len());
        assert!(matches!(
            igrf12_coefficients_for_date(
                Date::from_calendar_date(1890, Month::January, 1).unwrap()
            ),
            Err(MeasureError::ModelError { .. })
        ));
    }
}
