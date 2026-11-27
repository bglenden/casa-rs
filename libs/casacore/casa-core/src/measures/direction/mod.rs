// SPDX-License-Identifier: LGPL-3.0-or-later
//! Direction measure: sky directions in various celestial reference frames.
//!
//! This module provides:
//!
//! - [`DirectionRef`] — celestial reference frame types (J2000, GALACTIC, etc.).
//! - [`MDirection`] — a sky direction stored as direction cosines with a reference
//!   frame, equivalent to C++ `MDirection`.
//!
//! Conversions between frames use rotation matrices (constant or epoch-dependent)
//! via the [`sofars`] crate's precession, nutation, and coordinate functions.
//!
//! # SOFA vs casacore's bespoke algorithms
//!
//! C++ casacore does **not** use the IAU SOFA library internally — it predates
//! SOFA and has its own implementations of precession (Euler angle polynomials),
//! nutation (custom series), and aberration (Stumpff polynomial series).
//! SOFA is only an optional casacore build dependency used for testing.
//!
//! This Rust implementation uses [`sofars`] (a Rust port of SOFA) for the
//! shared precession, nutation, and ephemeris building blocks. The
//! apparent-place branch preserves casacore's observable semantics by using
//! its Stumpff aberration series and explicit solar-deflection hop:
//!
//! | IAU model | Typical deviation | Main contributors |
//! |-----------|-------------------|-------------------|
//! | 1976/1980 | ~1 µas | residual series decomposition |
//! | 2000A | ~16 mas | Precession/nutation parameterization |
//!
//! The affected conversion paths are the ones that pass through apparent place:
//! - `J2000 -> APP`
//! - `J2000 -> HADEC`
//! - `J2000 -> AZEL`
//! - `J2000 -> ITRF`
//!
//! `HADEC`, `AZEL`, and `ITRF` inherit the same mismatch because their
//! implementations transit the apparent-place branch before applying local
//! sidereal-time, horizon, or terrestrial rotations.
//!
//! The 1976/1980 path ports the two effects that otherwise dominate the
//! difference:
//! - **Sun gravitational deflection**: the `J2000 ↔ JNAT` hop follows
//!   casacore `MeasMath::applySolarPos` / `deapplySolarPos`.
//! - **Earth velocity**: the `JNAT ↔ APP` hop uses casacore's Stumpff
//!   polynomial velocity series and relativistic aberration transform.
//!
//! The larger IAU 2000A deviation (~16 mas) comes from differences in how
//! casacore and SOFA parameterize the IAU 2000 precession-nutation model.
//! Casacore uses (ζA, zA, θA) Euler angles with frame bias baked into the
//! constant terms; SOFA uses (ψA, ωA, χA) Lieske 1977 angles with IAU 2000
//! corrections from `pr00` and separate frame bias via `bi00`.
//! Upstream casacore PR `#1464` records the same working hypothesis for the
//! IAU2000 `JNAT <-> APP` helper: a missing `frameBias00()` step in the shared
//! combined precession/nutation path. That PR is useful background, but not
//! independent confirmation. If that fix lands upstream, the IAU2000A residual
//! should collapse from ~16 mas to the same ~1.5 mas class as the older
//! IAU 1976/1980 path.
//!
//! A test program comparing casacore and ERFA (SOFA) side-by-side is in
//! `misc/casacore_vs_sofa_deviation.cpp`.
//!
//! # Implemented routes
//!
//! **Constant-matrix** (no frame data needed), with casacore's matrices
//! (`casacore_frames`): J2000 ↔ GALACTIC, GALACTIC ↔ B1950, GALACTIC ↔
//! SUPERGAL, J2000 ↔ ECLIPTIC (casacore's J2000 obliquity, no frame bias),
//! J2000 ↔ ICRS, AZEL ↔ AZELSW, AZELGEO ↔ AZELSWGEO
//!
//! **FK4**: J2000 ↔ B1950 follows casacore's rotation, E-terms and epoch
//! term; the epoch is the frame's UT1 epoch, or J2000.0 without one.
//!
//! **Epoch-dependent** (need epoch in frame):
//! J2000 ↔ JMEAN (precession), JMEAN ↔ JTRUE (nutation),
//! JMEAN ↔ MECLIPTIC, JTRUE ↔ TECLIPTIC (casacore's IAU 1980 obliquities
//! under either model), J2000 ↔ JNAT (solar deflection), JNAT ↔ APP
//! (aberration plus precession/nutation)
//!
//! **Epoch + position** (need both):
//! APP ↔ TOPO (unchanged for a sky direction), TOPO ↔ HADEC (diurnal
//! aberration, polar motion and sidereal time), HADEC ↔ AZEL,
//! HADEC ↔ AZELGEO, HADEC ↔ ITRF
//!
//! # Routes that differ from casacore's
//!
//! Every route between the frames here follows casacore's routing table
//! (`MCDirection::ToRef_p`), except that casacore converts B1950 to and from
//! APP directly, with its B1950 precession, nutation and aberration, and so
//! B1950 to and from TOPO, HADEC, the AZEL frames and ITRF. casa-rs goes
//! through J2000 for the ordinary infinite-distance rotation, which differs
//! by 2.5–4.9″ (2026-10-22, #863). Evaluated ephemerides with a distance
//! follow the weighted casacore routes, including the direct FK4 pair.
//! BMEAN/BTRUE and B1950_VLA follow the corresponding FK4 routines.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::str::FromStr;
use std::sync::LazyLock;

use crate::quanta::MvAngle;
use crate::quanta::{Quantity, Unit};

use super::casacore_aberration::earth_barycentric_velocity_ms;
use super::casacore_frames;
use super::casacore_nutation::standard_true_obliquity_rad;
use super::epoch::{EpochRef, MEpoch, MISSING_DUT1, MjdHighPrec, casacore_gast_rad};
use super::error::MeasureError;
use super::frame::{IauModel, MeasFrame};

mod conversion;
mod frames;
mod named;

use conversion::*;
pub(super) use frames::casacore_aberration_beta;
use frames::*;
use named::*;

/// Speed of light in AU/day (IAU 2012 nominal).
const C_AU_PER_DAY: f64 = 173.144_632_674_240_34;

/// Exact SI speed of light used by casacore's aberration normalization.
const C_M_PER_S: f64 = 299_792_458.0;

/// MJD offset for JD pairs.
const MJD_OFFSET: f64 = 2_400_000.5;

// ---------------------------------------------------------------------------
// DirectionRef
// ---------------------------------------------------------------------------

/// Celestial reference frame types for direction measures.
///
/// Corresponds to C++ `MDirection::Types`, including dynamical body labels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DirectionRef {
    /// Mean equator/equinox at J2000.0 (dynamical).
    J2000,
    /// Mean equator/equinox of date (precessed J2000).
    JMEAN,
    /// True equator/equinox of date (precessed + nutated).
    JTRUE,
    /// Apparent place (true equatorial + aberration).
    APP,
    /// J2000 natural direction (geometric, before gravitational deflection).
    JNAT,
    /// Mean equator/equinox at B1950.0 (FK4, E-terms included).
    B1950,
    /// VLA's FK4 convention, with its fixed 1979.9 epoch term.
    B1950Vla,
    /// Mean equator/equinox of date under FK4 precession.
    BMEAN,
    /// True equator/equinox of date under FK4 precession/nutation.
    BTRUE,
    /// Galactic coordinates (IAU 1958).
    GALACTIC,
    /// Supergalactic coordinates (de Vaucouleurs 1991).
    SUPERGAL,
    /// International Celestial Reference System.
    ICRS,
    /// Ecliptic coordinates (casacore's mean obliquity of J2000.0).
    ECLIPTIC,
    /// Mean ecliptic of date.
    MECLIPTIC,
    /// True ecliptic of date.
    TECLIPTIC,
    /// Hour angle / declination.
    HADEC,
    /// Azimuth / elevation (N through E).
    AZEL,
    /// Azimuth / elevation (S through W).
    AZELSW,
    /// Azimuth / elevation (geodetic latitude, N through E).
    AZELGEO,
    /// Azimuth / elevation (geodetic latitude, S through W).
    AZELSWGEO,
    /// International Terrestrial Reference Frame direction.
    ITRF,
    /// Topocentric apparent right ascension and declination. For a sky
    /// direction, which has no distance, it equals APP, as in casacore.
    TOPO,
    /// CASA dynamical direction label MERCURY.
    MERCURY,
    /// CASA dynamical direction label VENUS.
    VENUS,
    /// CASA dynamical direction label MARS.
    MARS,
    /// CASA dynamical direction label JUPITER.
    JUPITER,
    /// CASA dynamical direction label SATURN.
    SATURN,
    /// CASA dynamical direction label URANUS.
    URANUS,
    /// CASA dynamical direction label NEPTUNE.
    NEPTUNE,
    /// CASA dynamical direction label PLUTO.
    PLUTO,
    /// CASA dynamical direction label SUN.
    SUN,
    /// CASA dynamical direction label MOON.
    MOON,
    /// CASA dynamical direction label COMET.
    COMET,
}

/// Total number of DirectionRef variants.
const NUM_DIR_REFS: usize = 22;

/// Resolve a CASA direction-frame name or unique prefix to its canonical name.
///
/// The returned name can be a CASA frame that [`DirectionRef`] does not yet
/// represent; aliases such as `AZELNE` resolve to their canonical spelling.
#[must_use]
pub fn casa_direction_frame_name(input: &str) -> Option<&'static str> {
    const NAMES: [(&str, &str); 35] = [
        ("J2000", "J2000"),
        ("JMEAN", "JMEAN"),
        ("JTRUE", "JTRUE"),
        ("APP", "APP"),
        ("B1950", "B1950"),
        ("B1950_VLA", "B1950_VLA"),
        ("BMEAN", "BMEAN"),
        ("BTRUE", "BTRUE"),
        ("GALACTIC", "GALACTIC"),
        ("HADEC", "HADEC"),
        ("AZEL", "AZEL"),
        ("AZELSW", "AZELSW"),
        ("AZELNE", "AZEL"),
        ("AZELGEO", "AZELGEO"),
        ("AZELSWGEO", "AZELSWGEO"),
        ("AZELNEGEO", "AZELGEO"),
        ("JNAT", "JNAT"),
        ("ECLIPTIC", "ECLIPTIC"),
        ("MECLIPTIC", "MECLIPTIC"),
        ("TECLIPTIC", "TECLIPTIC"),
        ("SUPERGAL", "SUPERGAL"),
        ("ITRF", "ITRF"),
        ("TOPO", "TOPO"),
        ("ICRS", "ICRS"),
        ("MERCURY", "MERCURY"),
        ("VENUS", "VENUS"),
        ("MARS", "MARS"),
        ("JUPITER", "JUPITER"),
        ("SATURN", "SATURN"),
        ("URANUS", "URANUS"),
        ("NEPTUNE", "NEPTUNE"),
        ("PLUTO", "PLUTO"),
        ("SUN", "SUN"),
        ("MOON", "MOON"),
        ("COMET", "COMET"),
    ];
    crate::quanta::unique_prefix_position(input, &NAMES, |(name, _)| name)
        .map(|index| NAMES[index].1)
}

impl DirectionRef {
    /// Returns the C++ casacore integer code for this reference type.
    ///
    /// These codes match the `MDirection::Types` enum values defined in C++
    /// `MDirection.h`. Note that the Rust enum ordering differs from C++ — use
    /// this method (not enum discriminants) for on-disk interoperability.
    pub fn casacore_code(self) -> i32 {
        match self {
            Self::J2000 => 0,
            Self::JMEAN => 1,
            Self::JTRUE => 2,
            Self::APP => 3,
            Self::B1950 => 4,
            Self::B1950Vla => 5,
            Self::BMEAN => 6,
            Self::BTRUE => 7,
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
            Self::MERCURY => 32,
            Self::VENUS => 33,
            Self::MARS => 34,
            Self::JUPITER => 35,
            Self::SATURN => 36,
            Self::URANUS => 37,
            Self::NEPTUNE => 38,
            Self::PLUTO => 39,
            Self::SUN => 40,
            Self::MOON => 41,
            Self::COMET => 42,
        }
    }

    /// Converts a C++ casacore integer code to the corresponding reference type.
    ///
    /// Returns `None` for unrecognised codes.
    pub fn from_casacore_code(code: i32) -> Option<Self> {
        match code {
            0 => Some(Self::J2000),
            1 => Some(Self::JMEAN),
            2 => Some(Self::JTRUE),
            3 => Some(Self::APP),
            4 => Some(Self::B1950),
            5 => Some(Self::B1950Vla),
            6 => Some(Self::BMEAN),
            7 => Some(Self::BTRUE),
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
            32 => Some(Self::MERCURY),
            33 => Some(Self::VENUS),
            34 => Some(Self::MARS),
            35 => Some(Self::JUPITER),
            36 => Some(Self::SATURN),
            37 => Some(Self::URANUS),
            38 => Some(Self::NEPTUNE),
            39 => Some(Self::PLUTO),
            40 => Some(Self::SUN),
            41 => Some(Self::MOON),
            42 => Some(Self::COMET),

            _ => None,
        }
    }

    /// Coordinate-system references in canonical order. Dynamical source labels
    /// use the ephemeris path rather than this rotation graph.
    pub const ALL: [DirectionRef; NUM_DIR_REFS] = [
        Self::J2000,
        Self::JMEAN,
        Self::JTRUE,
        Self::APP,
        Self::JNAT,
        Self::B1950,
        Self::B1950Vla,
        Self::BMEAN,
        Self::BTRUE,
        Self::GALACTIC,
        Self::SUPERGAL,
        Self::ICRS,
        Self::ECLIPTIC,
        Self::MECLIPTIC,
        Self::TECLIPTIC,
        Self::HADEC,
        Self::AZEL,
        Self::AZELSW,
        Self::AZELGEO,
        Self::AZELSWGEO,
        Self::ITRF,
        Self::TOPO,
    ];

    /// Returns the canonical string name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::J2000 => "J2000",
            Self::JMEAN => "JMEAN",
            Self::JTRUE => "JTRUE",
            Self::APP => "APP",
            Self::JNAT => "JNAT",
            Self::B1950 => "B1950",
            Self::B1950Vla => "B1950_VLA",
            Self::BMEAN => "BMEAN",
            Self::BTRUE => "BTRUE",
            Self::GALACTIC => "GALACTIC",
            Self::SUPERGAL => "SUPERGAL",
            Self::ICRS => "ICRS",
            Self::ECLIPTIC => "ECLIPTIC",
            Self::MECLIPTIC => "MECLIPTIC",
            Self::TECLIPTIC => "TECLIPTIC",
            Self::HADEC => "HADEC",
            Self::AZEL => "AZEL",
            Self::AZELSW => "AZELSW",
            Self::AZELGEO => "AZELGEO",
            Self::AZELSWGEO => "AZELSWGEO",
            Self::ITRF => "ITRF",
            Self::TOPO => "TOPO",
            Self::MERCURY => "MERCURY",
            Self::VENUS => "VENUS",
            Self::MARS => "MARS",
            Self::JUPITER => "JUPITER",
            Self::SATURN => "SATURN",
            Self::URANUS => "URANUS",
            Self::NEPTUNE => "NEPTUNE",
            Self::PLUTO => "PLUTO",
            Self::SUN => "SUN",
            Self::MOON => "MOON",
            Self::COMET => "COMET",
        }
    }
}

impl FromStr for DirectionRef {
    type Err = MeasureError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let references = [
            ("J2000", Self::J2000),
            ("JMEAN", Self::JMEAN),
            ("JTRUE", Self::JTRUE),
            ("APP", Self::APP),
            ("JNAT", Self::JNAT),
            ("B1950", Self::B1950),
            ("B1950_VLA", Self::B1950Vla),
            ("BMEAN", Self::BMEAN),
            ("BTRUE", Self::BTRUE),
            ("GALACTIC", Self::GALACTIC),
            ("GAL", Self::GALACTIC),
            ("SUPERGAL", Self::SUPERGAL),
            ("ICRS", Self::ICRS),
            ("ECLIPTIC", Self::ECLIPTIC),
            ("ECL", Self::ECLIPTIC),
            ("MECLIPTIC", Self::MECLIPTIC),
            ("TECLIPTIC", Self::TECLIPTIC),
            ("HADEC", Self::HADEC),
            ("AZEL", Self::AZEL),
            ("AZELSW", Self::AZELSW),
            ("AZELGEO", Self::AZELGEO),
            ("AZELSWGEO", Self::AZELSWGEO),
            ("ITRF", Self::ITRF),
            ("TOPO", Self::TOPO),
            ("MERCURY", Self::MERCURY),
            ("VENUS", Self::VENUS),
            ("MARS", Self::MARS),
            ("JUPITER", Self::JUPITER),
            ("SATURN", Self::SATURN),
            ("URANUS", Self::URANUS),
            ("NEPTUNE", Self::NEPTUNE),
            ("PLUTO", Self::PLUTO),
            ("SUN", Self::SUN),
            ("MOON", Self::MOON),
            ("COMET", Self::COMET),
        ];
        references
            .into_iter()
            .find_map(|(name, reference)| s.eq_ignore_ascii_case(name).then_some(reference))
            .ok_or_else(|| MeasureError::UnknownRefType {
                input: s.to_owned(),
            })
    }
}

impl fmt::Display for DirectionRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// MDirection
// ---------------------------------------------------------------------------

/// A sky direction in a specified celestial reference frame.
///
/// `MDirection` stores a direction as a unit vector (direction cosines)
/// `[x, y, z]` where:
/// - `x = cos(lat) cos(lon)`
/// - `y = cos(lat) sin(lon)`
/// - `z = sin(lat)`
///
/// This is the Rust equivalent of C++ `casa::MDirection`.
///
/// # Conversions
///
/// Use [`convert_to`](MDirection::convert_to) to transform between reference
/// frames. Some conversions require epoch and/or position data in the
/// [`MeasFrame`]:
///
/// - Precession/nutation: requires `epoch`
/// - Aberration: requires `epoch` (for Earth velocity)
/// - HADEC/AZEL: requires `epoch` (sidereal time) and `position` (latitude)
///
/// # Examples
///
/// ```
/// use casa_core::measures::direction::{MDirection, DirectionRef};
/// use casa_core::measures::MeasFrame;
///
/// // Galactic center (approximately l=0, b=0)
/// let gc = MDirection::from_angles(0.0, 0.0, DirectionRef::GALACTIC);
/// let frame = MeasFrame::new();
/// let j2000 = gc.convert_to(DirectionRef::J2000, &frame).unwrap();
/// // Should be near RA~266°, Dec~-29°
/// let (ra, dec) = j2000.as_angles();
/// assert!((ra.to_degrees() - 266.4).abs() < 1.0);
/// assert!((dec.to_degrees() + 28.9).abs() < 1.0);
/// ```
#[derive(Debug, Clone)]
pub struct MDirection {
    cosines: [f64; 3],
    refer: DirectionRef,
}

impl MDirection {
    /// Creates a direction from longitude and latitude in radians.
    pub fn from_angles(lon_rad: f64, lat_rad: f64, refer: DirectionRef) -> Self {
        Self {
            cosines: sofars::vm::s2c(lon_rad, lat_rad),
            refer,
        }
    }

    /// Creates a direction from direction cosines (unit vector).
    ///
    /// The input is normalized to unit length.
    pub fn from_cosines(cosines: [f64; 3], refer: DirectionRef) -> Self {
        let (_, unit) = sofars::vm::pn(&cosines);
        Self {
            cosines: unit,
            refer,
        }
    }

    /// Returns the direction cosines (unit vector).
    pub fn cosines(&self) -> [f64; 3] {
        self.cosines
    }

    /// Returns the reference frame.
    pub fn refer(&self) -> DirectionRef {
        self.refer
    }

    /// Relabel this direction's reference frame without converting its value.
    ///
    /// The direction cosines are preserved exactly. This is the operation
    /// casacore's `MDirection::set(Ref)` performs; it is distinct from a
    /// physical frame conversion.
    pub fn set_reference(&mut self, refer: DirectionRef) {
        self.refer = refer;
    }

    /// Returns the direction as (longitude, latitude) in radians.
    ///
    /// Longitude is in [0, 2π), latitude in [−π/2, π/2].
    pub fn as_angles(&self) -> (f64, f64) {
        let (lon, lat) = sofars::vm::c2s(&self.cosines);
        (sofars::vm::anp(lon), lat)
    }

    /// Returns the longitude in radians, in [0, 2π).
    pub fn longitude_rad(&self) -> f64 {
        self.as_angles().0
    }

    /// Returns the latitude in radians, in [−π/2, π/2].
    pub fn latitude_rad(&self) -> f64 {
        self.as_angles().1
    }

    /// Formats the longitude as conventional right ascension.
    ///
    /// This mirrors the common C++ casacore usage
    /// `MVAngle(ra)(0.0).string(MVAngle::TIME, 6 + second_decimals)`.
    pub fn format_right_ascension(&self, second_decimals: usize) -> String {
        format_right_ascension(self.longitude_rad(), second_decimals)
    }

    /// Formats the longitude as conventional right ascension with an explicit
    /// display-unit label.
    pub fn format_right_ascension_labeled(&self, second_decimals: usize) -> String {
        format_right_ascension_labeled(self.longitude_rad(), second_decimals)
    }

    /// Formats the latitude as conventional declination.
    ///
    /// This mirrors the common C++ casacore usage
    /// `MVAngle(dec).string(MVAngle::DIG2, 6 + second_decimals)`.
    pub fn format_declination(&self, second_decimals: usize) -> String {
        format_declination(self.latitude_rad(), second_decimals)
    }

    /// Formats the latitude as conventional declination with an explicit
    /// display-unit label.
    pub fn format_declination_labeled(&self, second_decimals: usize) -> String {
        format_declination_labeled(self.latitude_rad(), second_decimals)
    }

    /// Converts this direction to a different reference frame.
    ///
    /// Uses BFS on the routing graph to find the shortest conversion path,
    /// then applies each hop sequentially. Epoch-derived values (TT, UT1,
    /// GAST) are computed once and cached across all hops via `ConvCtx`.
    pub fn convert_to(
        &self,
        target: DirectionRef,
        frame: &MeasFrame,
    ) -> Result<MDirection, MeasureError> {
        self.convert_with_parallax(target, frame, 0.0)
    }

    /// Converts a direction with a known geocentric distance in metres.
    ///
    /// Casacore applies the observer displacement when the conversion
    /// crosses between geocentric apparent and topocentric coordinates.
    /// A distance of zero denotes a direction at infinity, as for
    /// [Self::convert_to]. Supply the distance of an already evaluated
    /// ephemeris direction, such as one in JNAT or APP.
    pub fn convert_with_parallax(
        &self,
        target: DirectionRef,
        frame: &MeasFrame,
        distance_m: f64,
    ) -> Result<MDirection, MeasureError> {
        if self.refer == target {
            return Ok(self.clone());
        }
        if self.refer.casacore_code() >= 32 {
            if self.refer == DirectionRef::COMET {
                return Err(MeasureError::MissingFrameData { what: "comet" });
            }
            return Self::from_source_name(self.refer.as_str(), frame)?.convert_to(target, frame);
        }
        if target.casacore_code() >= 32 {
            // MCDirection routes a dynamical output through J2000 and keeps
            // the requested label; it does not resolve that output body.
            let mut converted =
                self.convert_with_parallax(DirectionRef::J2000, frame, distance_m)?;
            converted.refer = target;
            return Ok(converted);
        }
        let ordinary;
        let path = if distance_m == 0.0 {
            ordinary = find_path(self.refer, target)?;
            ordinary.as_slice()
        } else {
            vector_rotation::route(self.refer, target)
        };
        let ctx = ConvCtx {
            distance_m,
            ..ConvCtx::new()
        };
        let mut cosines = self.cosines;
        let mut current_ref = self.refer;

        for &next_ref in path {
            cosines = apply_hop(cosines, current_ref, next_ref, frame, &ctx)?;
            current_ref = next_ref;
        }

        Ok(MDirection {
            cosines,
            refer: target,
        })
    }

    /// Resolves a casacore/CASA source name into a direction.
    ///
    /// This mirrors C++ `MDirection::makeMDirection`, supporting the built-in
    /// fixed calibrators (`CasA`, `CygA`, etc.), `ZENITH`, and the moving
    /// solar-system names that casacore exposes through TaQL `meas.*`
    /// direction UDFs.
    ///
    /// Moving bodies require an epoch in `frame`; fixed sources do not.
    pub fn from_source_name(source_name: &str, frame: &MeasFrame) -> Result<Self, MeasureError> {
        resolve_named_direction(&parse_named_direction(source_name, frame)?, frame)
    }

    /// Whether [`Self::from_source_name`] knows `source_name` and has a
    /// model of it: `ZENITH`, a solar-system body, a source of `frame`'s
    /// Measures provider or a built-in calibrator. Unlike resolving it,
    /// this needs no epoch; a solar-system body still fails to resolve at
    /// an epoch its ephemeris does not span (1900 to 2100).
    ///
    /// # Errors
    ///
    /// The error resolving `source_name` would give at every epoch: for a
    /// name it does not know or cannot parse, or a body it has no model of.
    pub fn check_source_name(source_name: &str, frame: &MeasFrame) -> Result<(), MeasureError> {
        match parse_named_direction(source_name, frame)?.kind {
            NamedDirectionKind::SolarSystem(SolarSystemBody::Pluto) => Err(pluto_unmodelled()),
            _ => Ok(()),
        }
    }

    /// Computes the rise and set UTC MJDs for this source.
    ///
    /// The frame must contain:
    /// - an epoch, used as the day-selection seed, and
    /// - an observatory position.
    ///
    /// This matches the TaQL `meas.riset` / `meas.riseset` scalar algorithm in
    /// C++ `DirectionEngine::calcRiseSet`, including its noon-based initial
    /// guess and the sidereal-day wrap adjustment.
    pub fn rise_set_times(&self, frame: &MeasFrame) -> Result<RiseSetTimes, MeasureError> {
        calc_rise_set(
            &|_trial_frame| Ok(self.clone()),
            default_rise_set_horizon_rad(&NamedDirectionKind::Fixed(self.clone())),
            frame,
        )
    }
}

/// Rise and set UTC MJDs returned by [`MDirection::rise_set_times`] and
/// [`rise_set_times_from_name`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RiseSetTimes {
    /// UTC MJD at which the source rises on the selected UTC day.
    pub rise_mjd: f64,
    /// UTC MJD at which the source sets on the selected UTC day.
    pub set_mjd: f64,
}

/// Computes the rise and set UTC MJDs for a source name.
///
/// This is the TaQL-facing named-source counterpart to
/// [`MDirection::rise_set_times`]. It accepts the same source-name spellings
/// and SUN/MOON suffixes that C++ `DirectionEngine::handleNames` uses for
/// `meas.riset` / `meas.riseset`.
pub fn rise_set_times_from_name(
    source_name: &str,
    frame: &MeasFrame,
) -> Result<RiseSetTimes, MeasureError> {
    let parsed = parse_named_direction(source_name, frame)?;
    calc_rise_set(
        &|trial_frame| resolve_named_direction(&parsed, trial_frame),
        parsed.rise_set_horizon_rad,
        frame,
    )
}

/// Formats a right-ascension value in radians as `hh:mm:ss[.f...]`.
///
/// This uses the existing `MvAngle` `TIME` formatter, which is already
/// cross-validated against C++ casacore in the quanta interop tests.
pub fn format_right_ascension(radians: f64, second_decimals: usize) -> String {
    MvAngle::from_radians(radians)
        .normalized(0.0)
        .format_time(second_decimals)
}

/// Formats a right-ascension value in radians as `hh:mm:ss[.f...] hms`.
pub fn format_right_ascension_labeled(radians: f64, second_decimals: usize) -> String {
    format!("{} hms", format_right_ascension(radians, second_decimals))
}

/// Formats a declination value in radians as signed `dd.mm.ss[.f...]`.
///
/// This uses the existing `MvAngle` `DIG2` formatter, which is already
/// cross-validated against C++ casacore in the quanta interop tests.
pub fn format_declination(radians: f64, second_decimals: usize) -> String {
    MvAngle::from_radians(radians).format_angle_dig2(second_decimals)
}

/// Formats a declination value in radians as signed `dd.mm.ss[.f...] dms`.
pub fn format_declination_labeled(radians: f64, second_decimals: usize) -> String {
    format!("{} dms", format_declination(radians, second_decimals))
}

/// Converts an angular increment in radians to an angular offset in arcseconds.
///
/// This matches casacore's default direction-coordinate unit model, where
/// direction axis increments are angular quantities even when right ascension
/// values themselves may be formatted as time-like strings for display.
pub fn angular_increment_arcseconds(radians: f64) -> Quantity {
    Quantity::with_unit(
        radians * (180.0 / std::f64::consts::PI) * 3_600.0,
        Unit::new("arcsec").expect("built-in arcsec unit must parse"),
    )
}

/// Converts an angular increment in radians to a right-ascension time offset.
pub fn right_ascension_increment_seconds(radians: f64) -> Quantity {
    Quantity::with_unit(
        radians * (86_400.0 / std::f64::consts::TAU),
        Unit::new("s").expect("built-in seconds unit must parse"),
    )
}

/// Converts an angular increment in radians to a declination offset.
pub fn declination_increment_arcseconds(radians: f64) -> Quantity {
    angular_increment_arcseconds(radians)
}

impl fmt::Display for MDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (lon, lat) = self.as_angles();
        write!(
            f,
            "Direction {}: lon={:.6}°, lat={:.6}°",
            self.refer,
            lon.to_degrees(),
            lat.to_degrees()
        )
    }
}

pub(super) mod vector_rotation;

#[cfg(test)]
mod tests;
