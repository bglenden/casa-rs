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
//! This Rust implementation uses [`sofars`] (a Rust port of SOFA) instead of
//! transliterating casacore's bespoke algorithms. Both implement the same IAU
//! standards but with different polynomial series and internal decompositions.
//! Measured deviations in J2000 → APP direction conversions:
//!
//! | IAU model | Typical deviation | Main contributors |
//! |-----------|-------------------|-------------------|
//! | 1976/1980 | ~1.5 mas | Sun deflection, velocity series |
//! | 2000A | ~16 mas | Precession/nutation parameterization |
//!
//! The IAU 1976/1980 deviation is dominated by:
//! - **Sun gravitational deflection**: C++ casacore applies full light deflection
//!   by the Sun (`applySolarPos`, up to ~1.75" at the limb, typically ~1–2 mas
//!   at moderate elongation). SOFA's `ab()` includes only a ~0.4 µas correction.
//! - **Earth velocity**: SOFA uses VSOP87 via `epv00`; casacore uses Stumpff
//!   polynomial series. Difference: ~0.1 mas.
//!
//! The larger IAU 2000A deviation (~16 mas) comes from differences in how
//! casacore and SOFA parameterize the IAU 2000 precession-nutation model.
//! Casacore uses (ζA, zA, θA) Euler angles with frame bias baked into the
//! constant terms; SOFA uses (ψA, ωA, χA) Lieske 1977 angles with IAU 2000
//! corrections from `pr00` and separate frame bias via `bi00`.
//!
//! A test program comparing casacore and ERFA (SOFA) side-by-side is in
//! `misc/casacore_vs_sofa_deviation.cpp`. An issue has been filed with the
//! casacore C++ maintainers to clarify whether this is expected.
//!
// TODO: The IAU 2000A ~16 mas deviation between casacore and SOFA needs
// investigation. Options: (1) transliterate casacore's IAU 2000 precession/
// nutation to match exactly, (2) determine which implementation is more
// accurate and document the difference, (3) add Sun gravitational deflection
// to reduce the IAU 1976 deviation to sub-mas.
//!
//! # Implemented routes
//!
//! **Constant-matrix** (no frame data needed):
//! J2000 ↔ GALACTIC, J2000 ↔ ICRS, GALACTIC ↔ SUPERGAL,
//! AZEL ↔ AZELSW, AZELGEO ↔ AZELSWGEO, JNAT ↔ J2000
//!
//! **Epoch-dependent** (need epoch in frame):
//! J2000 ↔ JMEAN (precession), JMEAN ↔ JTRUE (nutation),
//! J2000 ↔ ECLIPTIC (obliquity), JMEAN ↔ MECLIPTIC, JTRUE ↔ TECLIPTIC,
//! JTRUE ↔ APP (aberration)
//!
//! **Epoch + position** (need both):
//! APP ↔ HADEC (sidereal time), HADEC ↔ AZEL, HADEC ↔ AZELGEO

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::str::FromStr;
use std::sync::LazyLock;

use super::epoch::EpochRef;
use super::error::MeasureError;
use super::frame::{IauModel, MeasFrame};

/// Speed of light in AU/day (IAU 2012 nominal).
const C_AU_PER_DAY: f64 = 173.144_632_674_240_34;

/// MJD offset for JD pairs.
const MJD_OFFSET: f64 = 2_400_000.5;

// ---------------------------------------------------------------------------
// DirectionRef
// ---------------------------------------------------------------------------

/// Celestial reference frame types for direction measures.
///
/// Corresponds to a subset of C++ `MDirection::Types`. Planet-based and
/// B1950 frames are declared but conversions are deferred.
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
    /// Mean equator/equinox at B1950.0 (FK4). Deferred.
    B1950,
    /// Galactic coordinates (IAU 1958).
    GALACTIC,
    /// Supergalactic coordinates (de Vaucouleurs 1991).
    SUPERGAL,
    /// International Celestial Reference System.
    ICRS,
    /// Ecliptic coordinates (mean obliquity of J2000.0).
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
    /// International Terrestrial Reference Frame direction. Deferred.
    ITRF,
    /// Topocentric direction. Deferred.
    TOPO,
}

/// Total number of DirectionRef variants.
const NUM_DIR_REFS: usize = 19;

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
        }
    }

    /// Converts a C++ casacore integer code to the corresponding reference type.
    ///
    /// Returns `None` for unrecognised codes. Codes 5 (B1950_VLA), 6 (BMEAN),
    /// and 7 (BTRUE) are not supported in this Rust implementation.
    pub fn from_casacore_code(code: i32) -> Option<Self> {
        match code {
            0 => Some(Self::J2000),
            1 => Some(Self::JMEAN),
            2 => Some(Self::JTRUE),
            3 => Some(Self::APP),
            4 => Some(Self::B1950),
            // 5 => B1950_VLA (not supported)
            // 6 => BMEAN (not supported)
            // 7 => BTRUE (not supported)
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
            _ => None,
        }
    }

    /// All reference types in canonical order.
    pub const ALL: [DirectionRef; NUM_DIR_REFS] = [
        Self::J2000,
        Self::JMEAN,
        Self::JTRUE,
        Self::APP,
        Self::JNAT,
        Self::B1950,
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
        }
    }
}

impl FromStr for DirectionRef {
    type Err = MeasureError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "J2000" => Ok(Self::J2000),
            "JMEAN" => Ok(Self::JMEAN),
            "JTRUE" => Ok(Self::JTRUE),
            "APP" => Ok(Self::APP),
            "JNAT" => Ok(Self::JNAT),
            "B1950" => Ok(Self::B1950),
            "GALACTIC" | "GAL" => Ok(Self::GALACTIC),
            "SUPERGAL" => Ok(Self::SUPERGAL),
            "ICRS" => Ok(Self::ICRS),
            "ECLIPTIC" | "ECL" => Ok(Self::ECLIPTIC),
            "MECLIPTIC" => Ok(Self::MECLIPTIC),
            "TECLIPTIC" => Ok(Self::TECLIPTIC),
            "HADEC" => Ok(Self::HADEC),
            "AZEL" => Ok(Self::AZEL),
            "AZELSW" => Ok(Self::AZELSW),
            "AZELGEO" => Ok(Self::AZELGEO),
            "AZELSWGEO" => Ok(Self::AZELSWGEO),
            "ITRF" => Ok(Self::ITRF),
            "TOPO" => Ok(Self::TOPO),
            _ => Err(MeasureError::UnknownRefType {
                input: s.to_owned(),
            }),
        }
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
/// use casacore_types::measures::direction::{MDirection, DirectionRef};
/// use casacore_types::measures::MeasFrame;
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
        if self.refer == target {
            return Ok(self.clone());
        }
        let path = find_path(self.refer, target)?;
        let ctx = ConvCtx::new();
        let mut cosines = self.cosines;
        let mut current_ref = self.refer;

        for next_ref in path {
            cosines = apply_hop(cosines, current_ref, next_ref, frame, &ctx)?;
            current_ref = next_ref;
        }

        Ok(MDirection {
            cosines,
            refer: target,
        })
    }
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

// ---------------------------------------------------------------------------
// Rotation matrix helpers
// ---------------------------------------------------------------------------

/// Apply rotation matrix to direction vector: result = R * v.
fn rotate(r: &[[f64; 3]; 3], v: &[f64; 3]) -> [f64; 3] {
    let mut result = [0.0; 3];
    sofars::vm::rxp(r, v, &mut result);
    result
}

/// Apply transpose of rotation matrix: result = R^T * v.
fn rotate_t(r: &[[f64; 3]; 3], v: &[f64; 3]) -> [f64; 3] {
    let mut result = [0.0; 3];
    sofars::vm::trxp(r, v, &mut result);
    result
}

// ---------------------------------------------------------------------------
// Constant rotation matrices
// ---------------------------------------------------------------------------

/// Galactic-to-ICRS rotation matrix (Hipparcos, Murray 1989).
///
/// This matrix converts direction cosines from the galactic coordinate system
/// to ICRS (≈ J2000). It is the transpose of the matrix used internally by
/// `sofars::coords::g2icrs`.
///
/// v_ICRS = GAL_TO_ICRS * v_GAL
const GAL_TO_ICRS: [[f64; 3]; 3] = [
    [
        -0.054_875_560_416_215_4,
        0.494_109_427_875_583_7,
        -0.867_666_149_019_004_7,
    ],
    [
        -0.873_437_090_234_885,
        -0.444_829_629_960_011_2,
        -0.198_076_373_431_201_5,
    ],
    [
        -0.483_835_015_548_713_2,
        0.746_982_244_497_281_2,
        0.455_983_776_175_066_9,
    ],
];

/// ICRS-to-J2000 frame tie rotation matrix (IAU 2006).
///
/// The frame tie between ICRS and the dynamical mean equator/equinox at
/// J2000.0 is a tiny rotation (~17 mas). We extract the frame bias matrix
/// `rb` from `sofars::pnp::bp06` evaluated at J2000.0.
///
/// v_J2000 = ICRS_TO_J2000 * v_ICRS
fn icrs_to_j2000_matrix() -> [[f64; 3]; 3] {
    // bp06 returns (rb, rp, rbp) where rb is the frame bias
    let (rb, _, _) = sofars::pnp::bp06(MJD_OFFSET, 51544.5);
    rb
}

/// Supergalactic-to-Galactic rotation matrix.
///
/// Constructed from the de Vaucouleurs (1991) definition:
/// - Supergalactic pole at (l, b) = (47.37°, 6.32°)
/// - Zero longitude at l = 137.37° (= 47.37° + 90°)
///
/// v_GAL = SG_TO_GAL * v_SG
fn supergal_to_gal_matrix() -> [[f64; 3]; 3] {
    let l = 47.37_f64.to_radians();
    let b = 6.32_f64.to_radians();
    let (sl, cl) = (l.sin(), l.cos());
    let (sb, cb) = (b.sin(), b.cos());

    // Columns: e_x = [-sin(l), cos(l), 0]
    //          e_y = [-sin(b)cos(l), -sin(b)sin(l), cos(b)]
    //          e_z = [cos(b)cos(l), cos(b)sin(l), sin(b)]
    [
        [-sl, -sb * cl, cb * cl],
        [cl, -sb * sl, cb * sl],
        [0.0, cb, sb],
    ]
}

// ---------------------------------------------------------------------------
// Epoch-dependent rotation matrices
// ---------------------------------------------------------------------------

/// Get TT JD pair from the frame epoch, converting if needed.
fn get_tt_jd(frame: &MeasFrame, ctx: &ConvCtx) -> Result<(f64, f64), MeasureError> {
    ctx.get_tt_jd(frame)
}

/// Compute TT JD pair (uncached, used by ConvCtx).
fn compute_tt_jd(frame: &MeasFrame) -> Result<(f64, f64), MeasureError> {
    let epoch = frame.epoch().ok_or(MeasureError::MissingFrameData {
        what: "epoch (for precession/nutation)",
    })?;

    if epoch.refer() == EpochRef::TT {
        return Ok(epoch.value().as_jd_pair());
    }

    let tt = epoch.convert_to(EpochRef::TT, frame)?;
    Ok(tt.value().as_jd_pair())
}

/// Precession matrix (including frame bias): GCRS/J2000 → JMEAN (mean equator of date).
///
/// Dispatches on the IAU model:
/// - [`Iau1976_1980`](IauModel::Iau1976_1980): `sofars::pnp::pmat76` (Lieske 1979)
/// - [`Iau2006_2000A`](IauModel::Iau2006_2000A): `sofars::pnp::pmat00` — IAU 2000
///   bias-precession matrix (GCRS → mean of date), matching C++ casacore's
///   `frameBias00 * Precession(IAU2000)`.
fn precession_matrix(frame: &MeasFrame, ctx: &ConvCtx) -> Result<[[f64; 3]; 3], MeasureError> {
    ctx.get_precession(frame)
}

/// Nutation matrix: JMEAN → JTRUE (true equator of date).
///
/// Dispatches on the IAU model:
/// - [`Iau1976_1980`](IauModel::Iau1976_1980): `sofars::pnp::nutm80` (Wahr 1981)
/// - [`Iau2006_2000A`](IauModel::Iau2006_2000A): `sofars::pnp::num00a` (Mathews 2002)
fn nutation_matrix(frame: &MeasFrame, ctx: &ConvCtx) -> Result<[[f64; 3]; 3], MeasureError> {
    ctx.get_nutation(frame)
}

/// Mean obliquity of the ecliptic at the frame epoch.
fn mean_obliquity(frame: &MeasFrame, ctx: &ConvCtx) -> Result<f64, MeasureError> {
    let (tt1, tt2) = get_tt_jd(frame, ctx)?;
    Ok(match frame.iau_model() {
        IauModel::Iau1976_1980 => sofars::pnp::obl80(tt1, tt2),
        IauModel::Iau2006_2000A => sofars::pnp::obl80(tt1, tt2),
    })
}

/// Ecliptic rotation matrix for J2000: equatorial → ecliptic.
///
/// Uses the ecliptic-to-equatorial matrix from `sofars::coords::ecm06`,
/// transposed to get equatorial → ecliptic.
fn ecliptic_matrix_j2000() -> [[f64; 3]; 3] {
    // ecm06 returns the GCRS-to-ecliptic rotation matrix (equatorial → ecliptic).
    sofars::coords::ecm06(MJD_OFFSET, 51544.5)
}

/// Build a rotation matrix for rotation about x-axis by obliquity.
/// This converts equatorial → ecliptic.
fn obliquity_rotation(eps: f64) -> [[f64; 3]; 3] {
    let mut r = [[0.0; 3]; 3];
    sofars::vm::ir(&mut r);
    sofars::vm::rx(eps, &mut r);
    r
}

/// Get the Earth's barycentric velocity in AU/day for aberration.
fn earth_velocity_au_per_day(
    frame: &MeasFrame,
    ctx: &ConvCtx,
) -> Result<([f64; 3], f64), MeasureError> {
    ctx.get_earth_vel(frame)
}

/// Get GAST (Greenwich Apparent Sidereal Time) from the frame.
///
/// Dispatches on the IAU model:
/// - [`Iau1976_1980`](IauModel::Iau1976_1980): GMST82 + equation of equinoxes 1994
///   (`sofars::erst::gst94`)
/// - [`Iau2006_2000A`](IauModel::Iau2006_2000A): GMST82 + equation of equinoxes
///   IAU 2000A (`sofars::erst::gmst82` + `sofars::erst::ee00a`).
///
///   This matches C++ casacore's `MCEpoch::UT1_GAST` conversion, which always
///   uses GMST82 for the mean sidereal time even in IAU 2000 mode, combined
///   with the IAU 2000A equation of equinoxes (including complementary terms).
fn get_gast(frame: &MeasFrame, ctx: &ConvCtx) -> Result<f64, MeasureError> {
    ctx.get_gast(frame)
}

/// Compute GAST (uncached, used by ConvCtx).
fn compute_gast(frame: &MeasFrame, ctx: &ConvCtx) -> Result<f64, MeasureError> {
    let (ut_a, ut_b) = ctx.get_ut1_jd(frame)?;

    Ok(match frame.iau_model() {
        IauModel::Iau1976_1980 => sofars::erst::gst94(ut_a, ut_b),
        IauModel::Iau2006_2000A => {
            let (tt_a, tt_b) = ctx.get_tt_jd(frame)?;
            let gmst = sofars::erst::gmst82(ut_a, ut_b);
            let ee = sofars::erst::ee00a(tt_a, tt_b);
            sofars::vm::anp(gmst + ee)
        }
    })
}

/// Get the observer geodetic latitude from the frame position.
fn get_latitude(frame: &MeasFrame) -> Result<f64, MeasureError> {
    let pos = frame.position().ok_or(MeasureError::MissingFrameData {
        what: "position (for observer latitude)",
    })?;
    Ok(pos.latitude_rad())
}

/// Get the observer geocentric latitude from the frame position.
///
/// C++ casacore uses geocentric latitude (`asin(z/r)`) for AZEL conversions,
/// and geodetic latitude for AZELGEO. The difference is ~10 arcminutes.
fn get_geocentric_latitude(frame: &MeasFrame) -> Result<f64, MeasureError> {
    let pos = frame.position().ok_or(MeasureError::MissingFrameData {
        what: "position (for observer geocentric latitude)",
    })?;
    Ok(pos.geocentric_latitude_rad())
}

/// Get UT1 JD pair from the frame epoch, converting if needed.
fn get_ut1_jd(frame: &MeasFrame, ctx: &ConvCtx) -> Result<(f64, f64), MeasureError> {
    ctx.get_ut1_jd(frame)
}

/// Compute UT1 JD pair (uncached, used by ConvCtx).
fn compute_ut1_jd(frame: &MeasFrame) -> Result<(f64, f64), MeasureError> {
    let epoch = frame.epoch().ok_or(MeasureError::MissingFrameData {
        what: "epoch (for Earth rotation)",
    })?;

    if epoch.refer() == EpochRef::UT1 {
        return Ok(epoch.value().as_jd_pair());
    }

    let ut1 = epoch.convert_to(EpochRef::UT1, frame)?;
    Ok(ut1.value().as_jd_pair())
}

/// Get polar motion (xp, yp) in radians from the frame's EOP table.
///
/// Falls back to (0, 0) if no EOP table is attached or the epoch is
/// outside the table range. The ~0.3" max polar motion produces ~0.3"
/// error when not corrected, which is acceptable as a graceful fallback.
fn get_polar_motion_rad(frame: &MeasFrame, epoch_mjd: f64) -> (f64, f64) {
    const ARCSEC_TO_RAD: f64 = std::f64::consts::PI / (180.0 * 3600.0);
    match frame.polar_motion_for_mjd(epoch_mjd) {
        Some((xp_arcsec, yp_arcsec)) => (xp_arcsec * ARCSEC_TO_RAD, yp_arcsec * ARCSEC_TO_RAD),
        None => (0.0, 0.0),
    }
}

/// Get the geocentric radius in metres from the frame position.
fn get_geocentric_radius(frame: &MeasFrame) -> Result<f64, MeasureError> {
    let pos = frame.position().ok_or(MeasureError::MissingFrameData {
        what: "position (for geocentric radius)",
    })?;
    let xyz = pos.as_itrf();
    Ok((xyz[0] * xyz[0] + xyz[1] * xyz[1] + xyz[2] * xyz[2]).sqrt())
}

/// Compute the diurnal aberration factor v/c for an observer.
///
/// Returns the ratio of the observer's rotational velocity to the speed of light.
/// C++ casacore computes: `(2π × radius) / SECinDAY × UTtoST(T) / c`.
/// The UTtoST ratio is approximately 1.00273790935 (sidereal/solar time ratio).
fn diurnal_aberration_factor(radius_m: f64) -> f64 {
    const C: f64 = 299_792_458.0; // speed of light in m/s
    const SIDEREAL_RATIO: f64 = 1.002_737_909_35;
    (2.0 * std::f64::consts::PI * radius_m) / 86400.0 * SIDEREAL_RATIO / C
}

/// Compute the polar motion Euler matrix R = Ry(xp) × Rx(yp) × Rz(angle).
///
/// Returns the full 3×3 rotation matrix. This matches C++ casacore's
/// `MeasTable::polarMotion(tdb)` with the third Euler angle set to LAST.
fn polar_motion_euler(xp: f64, yp: f64, last: f64) -> [[f64; 3]; 3] {
    // Ry(xp) × Rx(yp) × Rz(last)
    let (sx, cx) = xp.sin_cos();
    let (sy, cy) = yp.sin_cos();
    let (sl, cl) = last.sin_cos();

    // Row 0 of Ry(xp):  [cx, 0, sx]
    // Row 1 of Ry(xp):  [0, 1, 0]
    // Row 2 of Ry(xp):  [-sx, 0, cx]
    //
    // Rx(yp): [1, 0, 0; 0, cy, -sy; 0, sy, cy]
    // Rz(last): [cl, -sl, 0; sl, cl, 0; 0, 0, 1]
    //
    // Compute Rx(yp) × Rz(last) first, then Ry(xp) × result.
    let rxz = [
        [cl, -sl, 0.0],
        [cy * sl, cy * cl, -sy],
        [sy * sl, sy * cl, cy],
    ];
    // Ry(xp) × rxz
    [
        [
            cx * rxz[0][0] + sx * rxz[2][0],
            cx * rxz[0][1] + sx * rxz[2][1],
            cx * rxz[0][2] + sx * rxz[2][2],
        ],
        rxz[1],
        [
            -sx * rxz[0][0] + cx * rxz[2][0],
            -sx * rxz[0][1] + cx * rxz[2][1],
            -sx * rxz[0][2] + cx * rxz[2][2],
        ],
    ]
}

/// Compute the bias-precession-nutation matrix, dispatching on IAU model.
///
/// - IAU 1976/1980: `pnm80` (Lieske precession + Wahr nutation, no frame bias)
/// - IAU 2006/2000A: `pnm00a` (IAU 2000 precession + IAU 2000A nutation + frame bias),
///   matching C++ casacore's combined `frameBias00 * Precession(IAU2000) * Nutation(IAU2000A)`.
fn bias_precession_nutation(frame: &MeasFrame, tt1: f64, tt2: f64) -> [[f64; 3]; 3] {
    match frame.iau_model() {
        IauModel::Iau1976_1980 => sofars::pnp::pnm80(tt1, tt2),
        IauModel::Iau2006_2000A => sofars::pnp::pnm00a(tt1, tt2),
    }
}

/// Get the observer longitude from the frame position.
fn get_longitude(frame: &MeasFrame) -> Result<f64, MeasureError> {
    let pos = frame.position().ok_or(MeasureError::MissingFrameData {
        what: "position (for observer longitude)",
    })?;
    Ok(pos.longitude_rad())
}

// ---------------------------------------------------------------------------
// Conversion routing graph
// ---------------------------------------------------------------------------

/// Edges in the direction conversion routing graph.
const ROUTING_EDGES: &[(DirectionRef, DirectionRef)] = &[
    (DirectionRef::J2000, DirectionRef::GALACTIC),
    (DirectionRef::J2000, DirectionRef::ICRS),
    (DirectionRef::GALACTIC, DirectionRef::SUPERGAL),
    (DirectionRef::J2000, DirectionRef::ECLIPTIC),
    (DirectionRef::J2000, DirectionRef::JMEAN),
    (DirectionRef::JMEAN, DirectionRef::JTRUE),
    (DirectionRef::JMEAN, DirectionRef::MECLIPTIC),
    (DirectionRef::JTRUE, DirectionRef::TECLIPTIC),
    (DirectionRef::JTRUE, DirectionRef::APP),
    (DirectionRef::JNAT, DirectionRef::J2000),
    (DirectionRef::HADEC, DirectionRef::AZEL),
    (DirectionRef::HADEC, DirectionRef::AZELGEO),
    (DirectionRef::AZEL, DirectionRef::AZELSW),
    (DirectionRef::AZELGEO, DirectionRef::AZELSWGEO),
    (DirectionRef::APP, DirectionRef::HADEC),
    (DirectionRef::J2000, DirectionRef::B1950),
    (DirectionRef::HADEC, DirectionRef::ITRF),
    (DirectionRef::HADEC, DirectionRef::TOPO),
];

/// Maps a DirectionRef to a unique index for BFS.
fn dir_ref_index(r: DirectionRef) -> usize {
    match r {
        DirectionRef::J2000 => 0,
        DirectionRef::JMEAN => 1,
        DirectionRef::JTRUE => 2,
        DirectionRef::APP => 3,
        DirectionRef::JNAT => 4,
        DirectionRef::B1950 => 5,
        DirectionRef::GALACTIC => 6,
        DirectionRef::SUPERGAL => 7,
        DirectionRef::ICRS => 8,
        DirectionRef::ECLIPTIC => 9,
        DirectionRef::MECLIPTIC => 10,
        DirectionRef::TECLIPTIC => 11,
        DirectionRef::HADEC => 12,
        DirectionRef::AZEL => 13,
        DirectionRef::AZELSW => 14,
        DirectionRef::AZELGEO => 15,
        DirectionRef::AZELSWGEO => 16,
        DirectionRef::ITRF => 17,
        DirectionRef::TOPO => 18,
    }
}

/// Pre-computed BFS paths for all reachable (source, target) pairs.
type DirPathMap = HashMap<(DirectionRef, DirectionRef), Option<Vec<DirectionRef>>>;

static DIR_PATH_CACHE: LazyLock<DirPathMap> = LazyLock::new(|| {
    let mut cache = HashMap::new();
    for &src in &DirectionRef::ALL {
        for &tgt in &DirectionRef::ALL {
            if src != tgt {
                cache.insert((src, tgt), bfs_find_dir_path(src, tgt));
            }
        }
    }
    cache
});

/// Finds the shortest path from `source` to `target` in the routing graph.
/// Results are cached in `DIR_PATH_CACHE`.
fn find_path(
    source: DirectionRef,
    target: DirectionRef,
) -> Result<Vec<DirectionRef>, MeasureError> {
    DIR_PATH_CACHE
        .get(&(source, target))
        .expect("all pairs pre-computed")
        .clone()
        .ok_or_else(|| MeasureError::NotYetImplemented {
            route: format!("{source} → {target} (no route found)"),
        })
}

/// BFS implementation used to populate the path cache.
fn bfs_find_dir_path(source: DirectionRef, target: DirectionRef) -> Option<Vec<DirectionRef>> {
    let mut visited = [false; NUM_DIR_REFS];
    let mut parent: [Option<DirectionRef>; NUM_DIR_REFS] = [None; NUM_DIR_REFS];
    let mut queue = VecDeque::new();

    let src_idx = dir_ref_index(source);
    visited[src_idx] = true;
    queue.push_back(source);

    let mut found = false;

    while let Some(current) = queue.pop_front() {
        if current == target {
            found = true;
            break;
        }

        for &(a, b) in ROUTING_EDGES {
            let neighbor = if a == current {
                b
            } else if b == current {
                a
            } else {
                continue;
            };
            let idx = dir_ref_index(neighbor);
            if !visited[idx] {
                visited[idx] = true;
                parent[idx] = Some(current);
                queue.push_back(neighbor);
            }
        }
    }

    if found {
        let mut path = Vec::new();
        let mut cur = target;
        while cur != source {
            path.push(cur);
            cur = parent[dir_ref_index(cur)].expect("BFS parent must be set");
        }
        path.reverse();
        Some(path)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Conversion context: caches epoch-derived values across hops
// ---------------------------------------------------------------------------

/// Caches expensive epoch-derived values (TT JD pair, UT1 JD pair, GAST)
/// so they are computed at most once per `convert_to` call.
struct ConvCtx {
    tt_jd: RefCell<Option<(f64, f64)>>,
    ut1_jd: RefCell<Option<(f64, f64)>>,
    gast: RefCell<Option<f64>>,
    precession: RefCell<Option<[[f64; 3]; 3]>>,
    nutation: RefCell<Option<[[f64; 3]; 3]>>,
    earth_vel: RefCell<Option<([f64; 3], f64)>>,
}

impl ConvCtx {
    fn new() -> Self {
        Self {
            tt_jd: RefCell::new(None),
            ut1_jd: RefCell::new(None),
            gast: RefCell::new(None),
            precession: RefCell::new(None),
            nutation: RefCell::new(None),
            earth_vel: RefCell::new(None),
        }
    }

    fn get_tt_jd(&self, frame: &MeasFrame) -> Result<(f64, f64), MeasureError> {
        if let Some(v) = *self.tt_jd.borrow() {
            return Ok(v);
        }
        let v = compute_tt_jd(frame)?;
        *self.tt_jd.borrow_mut() = Some(v);
        Ok(v)
    }

    fn get_ut1_jd(&self, frame: &MeasFrame) -> Result<(f64, f64), MeasureError> {
        if let Some(v) = *self.ut1_jd.borrow() {
            return Ok(v);
        }
        let v = compute_ut1_jd(frame)?;
        *self.ut1_jd.borrow_mut() = Some(v);
        Ok(v)
    }

    fn get_gast(&self, frame: &MeasFrame) -> Result<f64, MeasureError> {
        if let Some(v) = *self.gast.borrow() {
            return Ok(v);
        }
        let v = compute_gast(frame, self)?;
        *self.gast.borrow_mut() = Some(v);
        Ok(v)
    }

    fn get_precession(&self, frame: &MeasFrame) -> Result<[[f64; 3]; 3], MeasureError> {
        if let Some(v) = *self.precession.borrow() {
            return Ok(v);
        }
        let (tt1, tt2) = self.get_tt_jd(frame)?;
        let v = match frame.iau_model() {
            IauModel::Iau1976_1980 => sofars::pnp::pmat76(tt1, tt2),
            IauModel::Iau2006_2000A => sofars::pnp::pmat00(tt1, tt2),
        };
        *self.precession.borrow_mut() = Some(v);
        Ok(v)
    }

    fn get_nutation(&self, frame: &MeasFrame) -> Result<[[f64; 3]; 3], MeasureError> {
        if let Some(v) = *self.nutation.borrow() {
            return Ok(v);
        }
        let (tt1, tt2) = self.get_tt_jd(frame)?;
        let v = match frame.iau_model() {
            IauModel::Iau1976_1980 => sofars::pnp::nutm80(tt1, tt2),
            IauModel::Iau2006_2000A => sofars::pnp::num00a(tt1, tt2),
        };
        *self.nutation.borrow_mut() = Some(v);
        Ok(v)
    }

    fn get_earth_vel(&self, frame: &MeasFrame) -> Result<([f64; 3], f64), MeasureError> {
        if let Some(v) = *self.earth_vel.borrow() {
            return Ok(v);
        }
        let (tt1, tt2) = self.get_tt_jd(frame)?;
        let (pvh, pvb) =
            sofars::eph::epv00(tt1, tt2).ok_or(MeasureError::SofarsError { code: -1 })?;
        let sun_dist = sofars::vm::pm(pvh[0]);
        let v = (pvb[1], sun_dist);
        *self.earth_vel.borrow_mut() = Some(v);
        Ok(v)
    }
}

// ---------------------------------------------------------------------------
// Hop implementations
// ---------------------------------------------------------------------------

/// Applies a single conversion hop.
fn apply_hop(
    cosines: [f64; 3],
    from: DirectionRef,
    to: DirectionRef,
    frame: &MeasFrame,
    ctx: &ConvCtx,
) -> Result<[f64; 3], MeasureError> {
    use DirectionRef::*;

    match (from, to) {
        // ----- Constant matrices -----

        // J2000 ↔ GALACTIC (via ICRS ≈ J2000 approximation)
        (J2000, GALACTIC) => Ok(rotate_t(&GAL_TO_ICRS, &cosines)),
        (GALACTIC, J2000) => Ok(rotate(&GAL_TO_ICRS, &cosines)),

        // J2000 ↔ ICRS (frame tie, ~17 mas)
        (ICRS, J2000) => {
            let m = icrs_to_j2000_matrix();
            Ok(rotate(&m, &cosines))
        }
        (J2000, ICRS) => {
            let m = icrs_to_j2000_matrix();
            Ok(rotate_t(&m, &cosines))
        }

        // GALACTIC ↔ SUPERGAL
        (SUPERGAL, GALACTIC) => {
            let m = supergal_to_gal_matrix();
            Ok(rotate(&m, &cosines))
        }
        (GALACTIC, SUPERGAL) => {
            let m = supergal_to_gal_matrix();
            Ok(rotate_t(&m, &cosines))
        }

        // AZEL ↔ AZELSW (sign flip: Az_SW = Az + π, El_SW = El)
        // In direction cosines: negate x and y, keep z.
        (AZEL, AZELSW) | (AZELSW, AZEL) => Ok([-cosines[0], -cosines[1], cosines[2]]),

        // AZELGEO ↔ AZELSWGEO (same sign flip)
        (AZELGEO, AZELSWGEO) | (AZELSWGEO, AZELGEO) => Ok([-cosines[0], -cosines[1], cosines[2]]),

        // JNAT ↔ J2000 (gravitational deflection — simplified: identity)
        (JNAT, J2000) | (J2000, JNAT) => Ok(cosines),

        // ----- Epoch-dependent matrices -----

        // J2000 → JMEAN (precession with frame bias)
        (J2000, JMEAN) => {
            let rp = precession_matrix(frame, ctx)?;
            Ok(rotate(&rp, &cosines))
        }
        (JMEAN, J2000) => {
            let rp = precession_matrix(frame, ctx)?;
            Ok(rotate_t(&rp, &cosines))
        }

        // JMEAN → JTRUE (nutation)
        (JMEAN, JTRUE) => {
            let rn = nutation_matrix(frame, ctx)?;
            Ok(rotate(&rn, &cosines))
        }
        (JTRUE, JMEAN) => {
            let rn = nutation_matrix(frame, ctx)?;
            Ok(rotate_t(&rn, &cosines))
        }

        // J2000 ↔ ECLIPTIC (obliquity at J2000.0)
        (J2000, ECLIPTIC) => {
            let m = ecliptic_matrix_j2000();
            Ok(rotate(&m, &cosines))
        }
        (ECLIPTIC, J2000) => {
            let m = ecliptic_matrix_j2000();
            Ok(rotate_t(&m, &cosines))
        }

        // JMEAN ↔ MECLIPTIC (mean obliquity at epoch)
        (JMEAN, MECLIPTIC) => {
            let eps = mean_obliquity(frame, ctx)?;
            let m = obliquity_rotation(eps);
            Ok(rotate(&m, &cosines))
        }
        (MECLIPTIC, JMEAN) => {
            let eps = mean_obliquity(frame, ctx)?;
            let m = obliquity_rotation(eps);
            Ok(rotate_t(&m, &cosines))
        }

        // JTRUE ↔ TECLIPTIC (true obliquity = mean + nutation)
        (JTRUE, TECLIPTIC) => {
            let (tt1, tt2) = get_tt_jd(frame, ctx)?;
            let eps0 = sofars::pnp::obl06(tt1, tt2);
            let (dpsi, deps) = sofars::pnp::nut06a(tt1, tt2);
            let eps_true = eps0 + deps;
            let _ = dpsi; // nutation in longitude not needed for obliquity rotation
            let m = obliquity_rotation(eps_true);
            Ok(rotate(&m, &cosines))
        }
        (TECLIPTIC, JTRUE) => {
            let (tt1, tt2) = get_tt_jd(frame, ctx)?;
            let eps0 = sofars::pnp::obl06(tt1, tt2);
            let (_, deps) = sofars::pnp::nut06a(tt1, tt2);
            let eps_true = eps0 + deps;
            let m = obliquity_rotation(eps_true);
            Ok(rotate_t(&m, &cosines))
        }

        // JTRUE → APP (aberration)
        (JTRUE, APP) => {
            let (v_au_day, sun_dist) = earth_velocity_au_per_day(frame, ctx)?;
            // Convert velocity to units of c
            let v = [
                v_au_day[0] / C_AU_PER_DAY,
                v_au_day[1] / C_AU_PER_DAY,
                v_au_day[2] / C_AU_PER_DAY,
            ];
            // Rotate velocity to true equatorial frame
            let (tt1, tt2) = get_tt_jd(frame, ctx)?;
            let rbpn = bias_precession_nutation(frame, tt1, tt2);
            let v_true = rotate(&rbpn, &v);
            // Reciprocal of Lorentz factor: sqrt(1 - |v|²)
            let v2 = v[0] * v[0] + v[1] * v[1] + v[2] * v[2];
            let bm1 = (1.0 - v2).sqrt();
            Ok(sofars::astro::ab(&cosines, &v_true, sun_dist, bm1))
        }
        // APP → JTRUE (inverse aberration — iterative)
        (APP, JTRUE) => {
            let (v_au_day, sun_dist) = earth_velocity_au_per_day(frame, ctx)?;
            let v = [
                v_au_day[0] / C_AU_PER_DAY,
                v_au_day[1] / C_AU_PER_DAY,
                v_au_day[2] / C_AU_PER_DAY,
            ];
            let (tt1, tt2) = get_tt_jd(frame, ctx)?;
            let rbpn = bias_precession_nutation(frame, tt1, tt2);
            let v_true = rotate(&rbpn, &v);
            let v2 = v[0] * v[0] + v[1] * v[1] + v[2] * v[2];
            let bm1 = (1.0 - v2).sqrt();

            // Iterative removal: start with apparent as guess for natural
            let mut pnat = cosines;
            for _ in 0..5 {
                let papp = sofars::astro::ab(&pnat, &v_true, sun_dist, bm1);
                // Correct: pnat += (cosines - papp)
                for k in 0..3 {
                    pnat[k] += cosines[k] - papp[k];
                }
                // Re-normalize
                let (_, unit) = sofars::vm::pn(&pnat);
                pnat = unit;
            }
            Ok(pnat)
        }

        // APP → HADEC (sidereal time + diurnal aberration + polar motion)
        //
        // Matches C++ casacore's TOPO→HADEC path (APP→TOPO is a no-op for
        // unit-vector directions). The steps are:
        //   1. Diurnal aberration: add v/c in zenith direction, renormalize
        //   2. Polar motion + LAST rotation via Euler R = Ry(xp)×Rx(yp)×Rz(LAST)
        //   3. Apply R^T and negate y to get (HA, Dec)
        (APP, HADEC) => {
            let gast = get_gast(frame, ctx)?;
            let lon = get_longitude(frame)?;
            let last = gast + lon;

            // Diurnal aberration: shift in direction of (LAST, geocentric_lat)
            let geo_lat = get_geocentric_latitude(frame)?;
            let radius = get_geocentric_radius(frame)?;
            let v_c = diurnal_aberration_factor(radius);
            let aberr_dir = sofars::vm::s2c(last, geo_lat);
            let shifted = [
                cosines[0] + v_c * aberr_dir[0],
                cosines[1] + v_c * aberr_dir[1],
                cosines[2] + v_c * aberr_dir[2],
            ];
            let (_, unit) = sofars::vm::pn(&shifted);

            // Polar motion + LAST rotation + y-negate via Euler matrix
            // C++ uses: in *= R(xp, Ry, yp, Rx, LAST, Rz); in.y = -in.y
            // which is: in = R^T * in, then negate y
            let (_, ut2) = get_ut1_jd(frame, ctx)?;
            let (xp, yp) = get_polar_motion_rad(frame, ut2);
            let r = polar_motion_euler(-xp, -yp, last);
            let result = rotate_t(&r, &unit);
            Ok([result[0], -result[1], result[2]])
        }

        // HADEC → APP (inverse of above)
        (HADEC, APP) => {
            let gast = get_gast(frame, ctx)?;
            let lon = get_longitude(frame)?;
            let last = gast + lon;

            // applyPolarMotion: negate y, then apply R
            let (_, ut2) = get_ut1_jd(frame, ctx)?;
            let (xp, yp) = get_polar_motion_rad(frame, ut2);
            let r = polar_motion_euler(-xp, -yp, last);
            let negated = [cosines[0], -cosines[1], cosines[2]];
            let rotated = rotate(&r, &negated);

            // Undo diurnal aberration
            let geo_lat = get_geocentric_latitude(frame)?;
            let radius = get_geocentric_radius(frame)?;
            let v_c = diurnal_aberration_factor(radius);
            let aberr_dir = sofars::vm::s2c(last, geo_lat);
            let unshifted = [
                rotated[0] - v_c * aberr_dir[0],
                rotated[1] - v_c * aberr_dir[1],
                rotated[2] - v_c * aberr_dir[2],
            ];
            let (_, unit) = sofars::vm::pn(&unshifted);
            Ok(unit)
        }

        // HADEC ↔ AZEL (horizon coordinates using geocentric latitude)
        // C++ casacore uses geocentric latitude for AZEL (MeasMath::applyHADECtoAZEL
        // calls getInfo(LAT), which is asin(z/r) from ITRF XYZ).
        (HADEC, AZEL) => {
            let lat = get_geocentric_latitude(frame)?;
            let (ha, dec) = sofars::vm::c2s(&cosines);
            let [az, el] = sofars::coords::hd2ae(ha, dec, lat);
            Ok(sofars::vm::s2c(az, el))
        }
        (AZEL, HADEC) => {
            let lat = get_geocentric_latitude(frame)?;
            let (az, el) = sofars::vm::c2s(&cosines);
            let [ha, dec] = sofars::coords::ae2hd(az, el, lat);
            Ok(sofars::vm::s2c(ha, dec))
        }

        // HADEC ↔ AZELGEO (horizon coordinates using geodetic latitude)
        // C++ casacore uses geodetic latitude for AZELGEO (MeasMath::applyHADECtoAZELGEO
        // calls getInfo(LATGEO), which is from WGS84 conversion).
        (HADEC, AZELGEO) => {
            let lat = get_latitude(frame)?;
            let (ha, dec) = sofars::vm::c2s(&cosines);
            let [az, el] = sofars::coords::hd2ae(ha, dec, lat);
            Ok(sofars::vm::s2c(az, el))
        }
        (AZELGEO, HADEC) => {
            let lat = get_latitude(frame)?;
            let (az, el) = sofars::vm::c2s(&cosines);
            let [ha, dec] = sofars::coords::ae2hd(az, el, lat);
            Ok(sofars::vm::s2c(ha, dec))
        }

        // ----- B1950 (FK4/FK5) -----

        // J2000 → B1950: FK5 to FK4 via sofars::star::fk54z
        (J2000, B1950) => {
            let (lon, lat) = sofars::vm::c2s(&cosines);
            let (r1950, d1950, _, _) = sofars::star::fk54z(lon, lat, 1950.0);
            Ok(sofars::vm::s2c(r1950, d1950))
        }
        // B1950 → J2000: FK4 to FK5 via sofars::star::fk45z
        (B1950, J2000) => {
            let (lon, lat) = sofars::vm::c2s(&cosines);
            let (r2000, d2000) = sofars::star::fk45z(lon, lat, 1950.0);
            Ok(sofars::vm::s2c(r2000, d2000))
        }

        // ----- ITRF (via HADEC, matching C++ MeasMath::applyHADECtoITRF) -----

        // HADEC → ITRF: rotate by -longitude around z, then negate y.
        //
        // C++ MeasMath::applyHADECtoITRF does `in *= Rz(lon)` (which means
        // `in = Rz(lon)^T * in = Rz(-lon) * in`) then negates y.
        (HADEC, ITRF) => {
            let lon = get_longitude(frame)?;
            let (s, c) = lon.sin_cos();
            // Rz(-lon) * in
            let x = c * cosines[0] + s * cosines[1];
            let y = -s * cosines[0] + c * cosines[1];
            let z = cosines[2];
            // Negate y
            Ok([x, -y, z])
        }
        // ITRF → HADEC: negate y, then rotate by +longitude around z.
        //
        // C++ MeasMath::deapplyHADECtoITRF negates y then does
        // `in = Rz(lon) * in`.
        (ITRF, HADEC) => {
            let lon = get_longitude(frame)?;
            let (s, c) = lon.sin_cos();
            // Negate y first
            let ny = -cosines[1];
            // Rz(lon) * in
            let x = c * cosines[0] - s * ny;
            let y = s * cosines[0] + c * ny;
            let z = cosines[2];
            Ok([x, y, z])
        }

        // ----- TOPO (diurnal aberration) -----

        // HADEC → TOPO: apply diurnal aberration
        // Diurnal aberration is very small (~0.3" max) and is due to the
        // observer's velocity from Earth rotation.
        (HADEC, TOPO) | (TOPO, HADEC) => {
            // Diurnal aberration is small enough that we can use the simplified
            // formula. For now, treat TOPO ≈ HADEC (identity), which is accurate
            // to ~0.3 arcsec. This matches the precision of most casacore uses.
            // A full implementation would need the observatory velocity vector
            // projected onto the sky plane.
            let pos = frame.position().ok_or(MeasureError::MissingFrameData {
                what: "position (for diurnal aberration)",
            })?;
            let itrf = pos.as_itrf();
            const OMEGA: f64 = 7.292_115e-5; // Earth angular velocity rad/s
            // Observatory velocity in ITRF
            let v_itrf = [-OMEGA * itrf[1], OMEGA * itrf[0], 0.0];

            // Get GAST for rotation to equatorial (dispatches on IAU model)
            let gast = get_gast(frame, ctx)?;
            let lon = get_longitude(frame)?;
            let last = gast + lon;

            // Rotate velocity from ITRF to HADEC frame
            let mut r = [[0.0; 3]; 3];
            sofars::vm::ir(&mut r);
            sofars::vm::rz(-last, &mut r);
            let mut v_hadec = [0.0; 3];
            sofars::vm::rxp(&r, &v_itrf, &mut v_hadec);

            // Speed of light
            const C: f64 = 299_792_458.0;

            // Diurnal aberration: Δp = v/c - (p·v/c)p (first order)
            let pdotv = sofars::vm::pdp(&cosines, &v_hadec) / C;

            if from == HADEC {
                // HADEC → TOPO: add aberration
                let mut result = [0.0; 3];
                for k in 0..3 {
                    result[k] = cosines[k] + v_hadec[k] / C - pdotv * cosines[k];
                }
                let (_, unit) = sofars::vm::pn(&result);
                Ok(unit)
            } else {
                // TOPO → HADEC: subtract aberration
                let mut result = [0.0; 3];
                for k in 0..3 {
                    result[k] = cosines[k] - v_hadec[k] / C + pdotv * cosines[k];
                }
                let (_, unit) = sofars::vm::pn(&result);
                Ok(unit)
            }
        }

        _ => Err(MeasureError::NotYetImplemented {
            route: format!("{from} → {to}"),
        }),
    }
}

// ---------------------------------------------------------------------------
// Public helper: get J2000 direction (used by frequency module)
// ---------------------------------------------------------------------------

impl MDirection {
    /// Returns this direction converted to J2000 cosines.
    ///
    /// This is used internally by frequency conversions to compute the
    /// dot product with velocity vectors defined in J2000.
    pub fn to_j2000(&self, frame: &MeasFrame) -> Result<[f64; 3], MeasureError> {
        if self.refer == DirectionRef::J2000 {
            return Ok(self.cosines);
        }
        let j2000 = self.convert_to(DirectionRef::J2000, frame)?;
        Ok(j2000.cosines)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::measures::epoch::MEpoch;

    fn close(a: f64, b: f64, tol: f64) -> bool {
        (a - b).abs() < tol
    }

    #[test]
    fn direction_ref_parse_all() {
        // Test non-deferred refs
        for r in [
            DirectionRef::J2000,
            DirectionRef::GALACTIC,
            DirectionRef::ICRS,
            DirectionRef::ECLIPTIC,
            DirectionRef::HADEC,
            DirectionRef::AZEL,
        ] {
            let parsed: DirectionRef = r.as_str().parse().unwrap();
            assert_eq!(parsed, r);
        }
    }

    #[test]
    fn direction_ref_aliases() {
        assert_eq!(
            "GAL".parse::<DirectionRef>().unwrap(),
            DirectionRef::GALACTIC
        );
        assert_eq!(
            "ECL".parse::<DirectionRef>().unwrap(),
            DirectionRef::ECLIPTIC
        );
    }

    #[test]
    fn from_angles_roundtrip() {
        let lon = 1.5_f64;
        let lat = 0.7_f64;
        let d = MDirection::from_angles(lon, lat, DirectionRef::J2000);
        let (lon2, lat2) = d.as_angles();
        assert!(close(lon2, lon, 1e-14));
        assert!(close(lat2, lat, 1e-14));
    }

    #[test]
    fn galactic_center_to_j2000() {
        // Galactic center (l=0, b=0) → RA ≈ 266.4°, Dec ≈ -28.9°
        let gc = MDirection::from_angles(0.0, 0.0, DirectionRef::GALACTIC);
        let frame = MeasFrame::new();
        let j = gc.convert_to(DirectionRef::J2000, &frame).unwrap();
        let (ra, dec) = j.as_angles();
        assert!(
            close(ra.to_degrees(), 266.4, 1.0),
            "RA = {}",
            ra.to_degrees()
        );
        assert!(
            close(dec.to_degrees(), -28.9, 1.0),
            "Dec = {}",
            dec.to_degrees()
        );
    }

    #[test]
    fn j2000_galactic_roundtrip() {
        let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
        let frame = MeasFrame::new();
        let gal = d.convert_to(DirectionRef::GALACTIC, &frame).unwrap();
        let back = gal.convert_to(DirectionRef::J2000, &frame).unwrap();
        let sep = sofars::vm::sepp(&d.cosines(), &back.cosines());
        assert!(sep < 1e-13, "roundtrip sep = {} rad", sep);
    }

    #[test]
    fn j2000_icrs_small_offset() {
        // The frame tie is ~17 mas. Check roundtrip.
        let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
        let frame = MeasFrame::new();
        let icrs = d.convert_to(DirectionRef::ICRS, &frame).unwrap();
        let back = icrs.convert_to(DirectionRef::J2000, &frame).unwrap();
        let sep = sofars::vm::sepp(&d.cosines(), &back.cosines());
        assert!(sep < 1e-12, "roundtrip sep = {} rad", sep);
    }

    #[test]
    fn galactic_supergal_roundtrip() {
        let d = MDirection::from_angles(0.8, 0.3, DirectionRef::GALACTIC);
        let frame = MeasFrame::new();
        let sg = d.convert_to(DirectionRef::SUPERGAL, &frame).unwrap();
        let back = sg.convert_to(DirectionRef::GALACTIC, &frame).unwrap();
        let sep = sofars::vm::sepp(&d.cosines(), &back.cosines());
        assert!(sep < 1e-14, "roundtrip sep = {} rad", sep);
    }

    #[test]
    fn azel_azelsw_roundtrip() {
        let d = MDirection::from_angles(0.5, 0.3, DirectionRef::AZEL);
        let frame = MeasFrame::new();
        let sw = d.convert_to(DirectionRef::AZELSW, &frame).unwrap();
        let back = sw.convert_to(DirectionRef::AZEL, &frame).unwrap();
        let sep = sofars::vm::sepp(&d.cosines(), &back.cosines());
        assert!(sep < 1e-15, "roundtrip sep = {} rad", sep);
    }

    #[test]
    fn b1950_j2000_roundtrip() {
        let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
        let frame = MeasFrame::new();
        let b1950 = d.convert_to(DirectionRef::B1950, &frame).unwrap();
        let back = b1950.convert_to(DirectionRef::J2000, &frame).unwrap();
        let sep = sofars::vm::sepp(&d.cosines(), &back.cosines());
        assert!(sep < 1e-6, "B1950 roundtrip sep = {} rad", sep);
    }

    #[test]
    fn j2000_jmean_needs_epoch() {
        let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
        let frame = MeasFrame::new(); // no epoch
        let result = d.convert_to(DirectionRef::JMEAN, &frame);
        assert!(matches!(result, Err(MeasureError::MissingFrameData { .. })));
    }

    #[test]
    fn j2000_jmean_roundtrip() {
        let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
        let epoch = MEpoch::from_mjd(51544.5, EpochRef::TT);
        let frame = MeasFrame::new().with_epoch(epoch);
        let jm = d.convert_to(DirectionRef::JMEAN, &frame).unwrap();
        let back = jm.convert_to(DirectionRef::J2000, &frame).unwrap();
        let sep = sofars::vm::sepp(&d.cosines(), &back.cosines());
        assert!(sep < 1e-12, "roundtrip sep = {} rad", sep);
    }

    #[test]
    fn j2000_ecliptic_roundtrip() {
        let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
        let frame = MeasFrame::new();
        let ecl = d.convert_to(DirectionRef::ECLIPTIC, &frame).unwrap();
        let back = ecl.convert_to(DirectionRef::J2000, &frame).unwrap();
        let sep = sofars::vm::sepp(&d.cosines(), &back.cosines());
        assert!(sep < 1e-14, "roundtrip sep = {} rad", sep);
    }

    #[test]
    fn identity_conversion() {
        let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
        let frame = MeasFrame::new();
        let same = d.convert_to(DirectionRef::J2000, &frame).unwrap();
        assert_eq!(d.cosines(), same.cosines());
    }

    #[test]
    fn casacore_code_roundtrip() {
        for r in DirectionRef::ALL {
            let code = r.casacore_code();
            assert_eq!(DirectionRef::from_casacore_code(code), Some(r));
        }
    }

    #[test]
    fn casacore_code_known_values() {
        assert_eq!(DirectionRef::J2000.casacore_code(), 0);
        assert_eq!(DirectionRef::B1950.casacore_code(), 4);
        assert_eq!(DirectionRef::GALACTIC.casacore_code(), 8);
        assert_eq!(DirectionRef::JNAT.casacore_code(), 14);
        assert_eq!(DirectionRef::ICRS.casacore_code(), 21);
    }

    #[test]
    fn casacore_code_unsupported() {
        // B1950_VLA=5, BMEAN=6, BTRUE=7 not in Rust enum
        assert_eq!(DirectionRef::from_casacore_code(5), None);
        assert_eq!(DirectionRef::from_casacore_code(6), None);
        assert_eq!(DirectionRef::from_casacore_code(7), None);
    }
}
