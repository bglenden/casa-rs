// SPDX-License-Identifier: LGPL-3.0-or-later
//! Epoch measure: high-precision time instants in various reference frames.
//!
//! This module provides:
//!
//! - [`MjdHighPrec`] — a high-precision Modified Julian Date stored as
//!   (whole days, fractional day), equivalent to C++ `MVEpoch`.
//! - [`EpochRef`] — the 12 time-scale reference types supported by casacore.
//! - [`MEpoch`] — a measure pairing a value with a reference frame, equivalent
//!   to C++ `MEpoch`.
//!
//! Conversions between reference frames use the IAU SOFA algorithms via the
//! [`sofars`] crate. The conversion engine finds the shortest path through a
//! routing graph (matching C++ `MCEpoch::ToRef_p`) and applies each hop
//! sequentially.

use std::collections::VecDeque;
use std::fmt;
use std::ops;
use std::str::FromStr;

use super::error::MeasureError;
use super::frame::MeasFrame;
use crate::quanta::{Quantity, Unit};

/// The Julian Date offset for MJD: JD = MJD + 2400000.5.
const MJD_OFFSET: f64 = 2_400_000.5;

/// Number of seconds in a day.
const SECONDS_PER_DAY: f64 = 86_400.0;

// ---------------------------------------------------------------------------
// MjdHighPrec
// ---------------------------------------------------------------------------

/// High-precision Modified Julian Date stored as (whole days, fractional day).
///
/// Equivalent to C++ `MVEpoch` which stores `(wDay, frDay)`. By keeping the
/// integer day and sub-day fraction separate, arithmetic preserves ~10⁻¹⁶ s
/// precision — far better than a single `f64` MJD which loses precision for
/// dates far from the epoch.
///
/// # Normalization
///
/// The constructor normalizes so that `frac` is in [0, 1). This ensures that
/// `day` is always a whole number and comparisons are straightforward.
///
/// # Examples
///
/// ```
/// use casacore_types::measures::MjdHighPrec;
///
/// // J2000.0 = MJD 51544.5
/// let j2000 = MjdHighPrec::from_mjd(51544.5);
/// assert!((j2000.as_mjd() - 51544.5).abs() < 1e-10);
/// ```
#[derive(Debug, Clone, Copy)]
pub struct MjdHighPrec {
    day: f64,
    frac: f64,
}

impl MjdHighPrec {
    /// Creates a new `MjdHighPrec` from integer day and fractional day parts.
    ///
    /// The fractional part is normalized to [0, 1) by adjusting the day count.
    pub fn new(day: f64, frac: f64) -> Self {
        let total_frac = frac;
        let extra_days = total_frac.floor();
        Self {
            day: day + extra_days,
            frac: total_frac - extra_days,
        }
    }

    /// Creates a `MjdHighPrec` by splitting a single MJD value.
    ///
    /// The integer part becomes `day` and the remainder becomes `frac`.
    pub fn from_mjd(mjd: f64) -> Self {
        let day = mjd.floor();
        Self {
            day,
            frac: mjd - day,
        }
    }

    /// Returns the combined MJD as a single `f64` (lossy for large values).
    pub fn as_mjd(&self) -> f64 {
        self.day + self.frac
    }

    /// Returns the whole-day part.
    pub fn day(&self) -> f64 {
        self.day
    }

    /// Returns the fractional-day part, in [0, 1).
    pub fn frac(&self) -> f64 {
        self.frac
    }

    /// Converts to a 2-part Julian Date suitable for sofars input.
    ///
    /// Returns `(2400000.5, day + frac)` so that JD1 + JD2 = JD.
    /// This is the standard convention used by SOFA/sofars for maximum precision.
    pub fn as_jd_pair(&self) -> (f64, f64) {
        (MJD_OFFSET, self.day + self.frac)
    }

    /// Creates a `MjdHighPrec` from a 2-part Julian Date.
    ///
    /// Expects `jd1 = 2400000.5` (the MJD offset) and `jd2 = MJD`.
    /// Other splits are accepted but may lose sub-day precision.
    pub fn from_jd_pair(jd1: f64, jd2: f64) -> Self {
        let mjd = (jd1 - MJD_OFFSET) + jd2;
        Self::from_mjd(mjd)
    }

    /// Returns the fractional day as seconds.
    pub fn frac_as_seconds(&self) -> f64 {
        self.frac * SECONDS_PER_DAY
    }
}

impl PartialEq for MjdHighPrec {
    fn eq(&self, other: &Self) -> bool {
        self.day == other.day && self.frac == other.frac
    }
}

impl ops::Add<f64> for MjdHighPrec {
    type Output = Self;
    /// Adds a number of days to this epoch.
    fn add(self, days: f64) -> Self {
        Self::new(self.day, self.frac + days)
    }
}

impl ops::Sub<f64> for MjdHighPrec {
    type Output = Self;
    /// Subtracts a number of days from this epoch.
    fn sub(self, days: f64) -> Self {
        Self::new(self.day, self.frac - days)
    }
}

impl ops::Sub for MjdHighPrec {
    type Output = f64;
    /// Returns the difference in days between two epochs.
    fn sub(self, other: Self) -> f64 {
        (self.day - other.day) + (self.frac - other.frac)
    }
}

impl fmt::Display for MjdHighPrec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_mjd())
    }
}

// ---------------------------------------------------------------------------
// EpochRef
// ---------------------------------------------------------------------------

/// Time-scale reference types for epoch measures.
///
/// These correspond to the 12 reference types in C++ `MEpoch::Types`.
/// Each represents a different time scale used in astronomy.
///
/// # Synonyms
///
/// When parsing from strings, the following synonyms are recognised:
/// - `IAT` → [`TAI`](EpochRef::TAI)
/// - `TDT`, `ET` → [`TT`](EpochRef::TT)
/// - `UT` → [`UT1`](EpochRef::UT1)
/// - `GMST` → [`GMST1`](EpochRef::GMST1)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EpochRef {
    /// Local Apparent Sidereal Time.
    LAST,
    /// Local Mean Sidereal Time.
    LMST,
    /// Greenwich Mean Sidereal Time (IAU 2006).
    GMST1,
    /// Greenwich Apparent Sidereal Time.
    GAST,
    /// Universal Time (UT1), corrected for polar motion.
    UT1,
    /// Universal Time (UT2), smoothed UT1.
    UT2,
    /// Coordinated Universal Time.
    UTC,
    /// International Atomic Time.
    TAI,
    /// Terrestrial Time (formerly TDT).
    TT,
    /// Geocentric Coordinate Time.
    TCG,
    /// Barycentric Dynamical Time.
    TDB,
    /// Barycentric Coordinate Time.
    TCB,
}

impl EpochRef {
    /// Returns the C++ casacore integer code for this reference type.
    ///
    /// These codes match the `MEpoch::Types` enum values defined in C++
    /// `MEpoch.h` and are used in `MEASINFO` variable-reference columns
    /// (`TabRefCodes`).
    pub fn casacore_code(self) -> i32 {
        match self {
            Self::LAST => 0,
            Self::LMST => 1,
            Self::GMST1 => 2,
            Self::GAST => 3,
            Self::UT1 => 4,
            Self::UT2 => 5,
            Self::UTC => 6,
            Self::TAI => 7,
            Self::TT => 8, // C++ TDT=8
            Self::TCG => 9,
            Self::TDB => 10,
            Self::TCB => 11,
        }
    }

    /// Converts a C++ casacore integer code to the corresponding reference type.
    ///
    /// Returns `None` for unrecognised codes.
    pub fn from_casacore_code(code: i32) -> Option<Self> {
        match code {
            0 => Some(Self::LAST),
            1 => Some(Self::LMST),
            2 => Some(Self::GMST1),
            3 => Some(Self::GAST),
            4 => Some(Self::UT1),
            5 => Some(Self::UT2),
            6 => Some(Self::UTC),
            7 => Some(Self::TAI),
            8 => Some(Self::TT),
            9 => Some(Self::TCG),
            10 => Some(Self::TDB),
            11 => Some(Self::TCB),
            _ => None,
        }
    }

    /// All 12 reference types in canonical order.
    pub const ALL: [EpochRef; 12] = [
        Self::LAST,
        Self::LMST,
        Self::GMST1,
        Self::GAST,
        Self::UT1,
        Self::UT2,
        Self::UTC,
        Self::TAI,
        Self::TT,
        Self::TCG,
        Self::TDB,
        Self::TCB,
    ];

    /// Returns the canonical string name of this reference type.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LAST => "LAST",
            Self::LMST => "LMST",
            Self::GMST1 => "GMST1",
            Self::GAST => "GAST",
            Self::UT1 => "UT1",
            Self::UT2 => "UT2",
            Self::UTC => "UTC",
            Self::TAI => "TAI",
            Self::TT => "TT",
            Self::TCG => "TCG",
            Self::TDB => "TDB",
            Self::TCB => "TCB",
        }
    }

    /// Returns the C++ casacore string name for this reference type.
    ///
    /// This differs from [`as_str`](Self::as_str) only for [`TT`](Self::TT),
    /// which C++ casacore stores as `"TDT"` in `TabRefTypes` arrays.
    pub fn casacore_name(self) -> &'static str {
        match self {
            Self::TT => "TDT",
            other => other.as_str(),
        }
    }
}

impl FromStr for EpochRef {
    type Err = MeasureError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "LAST" => Ok(Self::LAST),
            "LMST" => Ok(Self::LMST),
            "GMST1" | "GMST" => Ok(Self::GMST1),
            "GAST" => Ok(Self::GAST),
            "UT1" | "UT" => Ok(Self::UT1),
            "UT2" => Ok(Self::UT2),
            "UTC" => Ok(Self::UTC),
            "TAI" | "IAT" => Ok(Self::TAI),
            "TT" | "TDT" | "ET" => Ok(Self::TT),
            "TCG" => Ok(Self::TCG),
            "TDB" => Ok(Self::TDB),
            "TCB" => Ok(Self::TCB),
            _ => Err(MeasureError::UnknownRefType {
                input: s.to_owned(),
            }),
        }
    }
}

impl fmt::Display for EpochRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// MEpoch
// ---------------------------------------------------------------------------

/// An epoch measure: a time instant in a specified reference frame.
///
/// `MEpoch` pairs a high-precision MJD value ([`MjdHighPrec`]) with a
/// time-scale reference ([`EpochRef`]). It is the Rust equivalent of C++
/// `casa::MEpoch`.
///
/// # Conversions
///
/// Use [`convert_to`](MEpoch::convert_to) to transform between reference
/// frames. Most conversions are handled by the IAU SOFA algorithms via
/// [`sofars`]. Some conversions require auxiliary data in a [`MeasFrame`]:
///
/// - UT1 ↔ UTC requires `dut1_seconds` (the UT1−UTC offset).
/// - GMST1 ↔ LMST requires a position (for the observatory longitude).
///
/// # Examples
///
/// ```
/// use casacore_types::measures::{MEpoch, EpochRef, MjdHighPrec, MeasFrame};
///
/// let utc = MEpoch::from_mjd(51544.5, EpochRef::UTC);
/// let frame = MeasFrame::new();
/// let tai = utc.convert_to(EpochRef::TAI, &frame).unwrap();
/// // TAI = UTC + 32s at J2000
/// let diff_s = (tai.value().as_mjd() - utc.value().as_mjd()) * 86400.0;
/// assert!((diff_s - 32.0).abs() < 0.01);
/// ```
#[derive(Debug, Clone)]
pub struct MEpoch {
    value: MjdHighPrec,
    refer: EpochRef,
}

impl MEpoch {
    /// Creates a new `MEpoch` from a high-precision value and reference type.
    pub fn new(value: MjdHighPrec, refer: EpochRef) -> Self {
        Self { value, refer }
    }

    /// Creates a new `MEpoch` from a single MJD value and reference type.
    pub fn from_mjd(mjd: f64, refer: EpochRef) -> Self {
        Self {
            value: MjdHighPrec::from_mjd(mjd),
            refer,
        }
    }

    /// Creates a new `MEpoch` from a [`Quantity`] (expected in time units) and
    /// reference type.
    ///
    /// The quantity is converted to days (MJD) before storing.
    pub fn from_quantity(q: &Quantity, refer: EpochRef) -> Result<Self, MeasureError> {
        let day_unit = Unit::new("d").expect("'d' is a valid unit");
        let mjd = q
            .get_value_in(&day_unit)
            .map_err(|_| MeasureError::NonConformantUnit {
                expected: "time",
                got: q.unit().name().to_owned(),
            })?;
        Ok(Self {
            value: MjdHighPrec::from_mjd(mjd),
            refer,
        })
    }

    /// Returns the high-precision value.
    pub fn value(&self) -> MjdHighPrec {
        self.value
    }

    /// Returns the reference type.
    pub fn refer(&self) -> EpochRef {
        self.refer
    }

    /// Returns the epoch value as a [`Quantity`] in days.
    pub fn as_quantity(&self) -> Quantity {
        Quantity::new(self.value.as_mjd(), "d").expect("'d' is a valid unit")
    }

    /// Converts this epoch to a different reference frame.
    ///
    /// Uses BFS on the routing graph to find the shortest conversion path,
    /// then applies each hop sequentially through sofars functions.
    ///
    /// Some conversions require data in the [`MeasFrame`]:
    /// - UT1 ↔ UTC: requires `dut1_seconds`
    /// - GMST1 ↔ LMST: requires `position` (for longitude)
    /// - UT1 → GMST1: requires the frame to derive TT
    pub fn convert_to(&self, target: EpochRef, frame: &MeasFrame) -> Result<MEpoch, MeasureError> {
        if self.refer == target {
            return Ok(self.clone());
        }
        let path = find_path(self.refer, target)?;
        let mut current = self.value;
        let mut current_ref = self.refer;

        for next_ref in path {
            current = apply_hop(current, current_ref, next_ref, frame)?;
            current_ref = next_ref;
        }

        Ok(MEpoch {
            value: current,
            refer: target,
        })
    }
}

impl fmt::Display for MEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Epoch: {} {}", self.value, self.refer)
    }
}

// ---------------------------------------------------------------------------
// Conversion engine
// ---------------------------------------------------------------------------

/// The edges in our conversion routing graph.
/// Each entry is (from, to). The graph is bidirectional — each hop function
/// handles both directions.
const ROUTING_EDGES: &[(EpochRef, EpochRef)] = &[
    (EpochRef::UTC, EpochRef::TAI),
    (EpochRef::TAI, EpochRef::TT),
    (EpochRef::TT, EpochRef::TDB),
    (EpochRef::TT, EpochRef::TCG),
    (EpochRef::TDB, EpochRef::TCB),
    (EpochRef::UT1, EpochRef::UTC),
    (EpochRef::UT1, EpochRef::GMST1),
    (EpochRef::GMST1, EpochRef::LMST),
    (EpochRef::UT1, EpochRef::GAST),
    (EpochRef::GAST, EpochRef::LAST),
];

/// Deferred edges that are not yet implemented.
const DEFERRED_EDGES: &[(EpochRef, EpochRef)] = &[(EpochRef::UT1, EpochRef::UT2)];

/// Finds the shortest path from `source` to `target` in the routing graph.
///
/// Returns the sequence of intermediate + final reference types to visit
/// (excluding the source).
fn find_path(source: EpochRef, target: EpochRef) -> Result<Vec<EpochRef>, MeasureError> {
    // Check if the target is reachable only through deferred edges.
    // First, try BFS on the implemented graph.
    let mut visited = [false; 12];
    let mut parent: [Option<EpochRef>; 12] = [None; 12];
    let mut queue = VecDeque::new();

    let src_idx = epoch_ref_index(source);
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
            let idx = epoch_ref_index(neighbor);
            if !visited[idx] {
                visited[idx] = true;
                parent[idx] = Some(current);
                queue.push_back(neighbor);
            }
        }
    }

    if found {
        // Reconstruct path from target back to source.
        let mut path = Vec::new();
        let mut cur = target;
        while cur != source {
            path.push(cur);
            cur = parent[epoch_ref_index(cur)].expect("BFS parent must be set");
        }
        path.reverse();
        return Ok(path);
    }

    // Check if a deferred edge would connect.
    for &(a, b) in DEFERRED_EDGES {
        if (a == source && b == target)
            || (b == source && a == target)
            || (visited[epoch_ref_index(a)] && b == target)
            || (visited[epoch_ref_index(b)] && a == target)
            || (a == source && visited[epoch_ref_index(b)])
            || (b == source && visited[epoch_ref_index(a)])
        {
            return Err(MeasureError::NotYetImplemented {
                route: format!("{source} → {target}"),
            });
        }
    }

    Err(MeasureError::NotYetImplemented {
        route: format!("{source} → {target} (no route found)"),
    })
}

/// Maps an `EpochRef` variant to a unique index 0..11 for the BFS arrays.
fn epoch_ref_index(r: EpochRef) -> usize {
    match r {
        EpochRef::LAST => 0,
        EpochRef::LMST => 1,
        EpochRef::GMST1 => 2,
        EpochRef::GAST => 3,
        EpochRef::UT1 => 4,
        EpochRef::UT2 => 5,
        EpochRef::UTC => 6,
        EpochRef::TAI => 7,
        EpochRef::TT => 8,
        EpochRef::TCG => 9,
        EpochRef::TDB => 10,
        EpochRef::TCB => 11,
    }
}

/// Applies a single conversion hop from `from_ref` to `to_ref`.
fn apply_hop(
    value: MjdHighPrec,
    from_ref: EpochRef,
    to_ref: EpochRef,
    frame: &MeasFrame,
) -> Result<MjdHighPrec, MeasureError> {
    use EpochRef::*;
    let (jd1, jd2) = value.as_jd_pair();

    match (from_ref, to_ref) {
        // UTC → TAI
        (UTC, TAI) => {
            let (r1, r2) =
                sofars::ts::utctai(jd1, jd2).map_err(|code| MeasureError::SofarsError { code })?;
            Ok(MjdHighPrec::from_jd_pair(r1, r2))
        }
        // TAI → UTC
        (TAI, UTC) => {
            let (r1, r2) =
                sofars::ts::taiutc(jd1, jd2).map_err(|code| MeasureError::SofarsError { code })?;
            Ok(MjdHighPrec::from_jd_pair(r1, r2))
        }
        // TAI → TT
        (TAI, TT) => {
            let (r1, r2) =
                sofars::ts::taitt(jd1, jd2).map_err(|code| MeasureError::SofarsError { code })?;
            Ok(MjdHighPrec::from_jd_pair(r1, r2))
        }
        // TT → TAI
        (TT, TAI) => {
            let (r1, r2) =
                sofars::ts::tttai(jd1, jd2).map_err(|code| MeasureError::SofarsError { code })?;
            Ok(MjdHighPrec::from_jd_pair(r1, r2))
        }
        // TT → TDB
        (TT, TDB) => {
            let dtr = compute_dtdb(value, frame);
            let (r1, r2) = sofars::ts::tttdb(jd1, jd2, dtr)
                .map_err(|code| MeasureError::SofarsError { code })?;
            Ok(MjdHighPrec::from_jd_pair(r1, r2))
        }
        // TDB → TT
        (TDB, TT) => {
            // For the inverse, we need dtr as well. Use approximate TDB≈TT for dtdb.
            let dtr = compute_dtdb(value, frame);
            let (r1, r2) = sofars::ts::tdbtt(jd1, jd2, dtr)
                .map_err(|code| MeasureError::SofarsError { code })?;
            Ok(MjdHighPrec::from_jd_pair(r1, r2))
        }
        // TT → TCG
        (TT, TCG) => {
            let (r1, r2) =
                sofars::ts::tttcg(jd1, jd2).map_err(|code| MeasureError::SofarsError { code })?;
            Ok(MjdHighPrec::from_jd_pair(r1, r2))
        }
        // TCG → TT
        (TCG, TT) => {
            let (r1, r2) =
                sofars::ts::tcgtt(jd1, jd2).map_err(|code| MeasureError::SofarsError { code })?;
            Ok(MjdHighPrec::from_jd_pair(r1, r2))
        }
        // TDB → TCB
        (TDB, TCB) => {
            let (r1, r2) =
                sofars::ts::tdbtcb(jd1, jd2).map_err(|code| MeasureError::SofarsError { code })?;
            Ok(MjdHighPrec::from_jd_pair(r1, r2))
        }
        // TCB → TDB
        (TCB, TDB) => {
            let (r1, r2) =
                sofars::ts::tcbtdb(jd1, jd2).map_err(|code| MeasureError::SofarsError { code })?;
            Ok(MjdHighPrec::from_jd_pair(r1, r2))
        }
        // UT1 → UTC: subtract dUT1
        (UT1, UTC) => {
            let dut1 =
                frame
                    .dut1_for_mjd(value.as_mjd())
                    .ok_or(MeasureError::MissingFrameData {
                        what: "dUT1 (UT1-UTC offset in seconds; set with_dut1() or with_eop())",
                    })?;
            // UT1 = UTC + dUT1, so UTC = UT1 - dUT1
            Ok(value - dut1 / SECONDS_PER_DAY)
        }
        // UTC → UT1: add dUT1
        (UTC, UT1) => {
            let dut1 =
                frame
                    .dut1_for_mjd(value.as_mjd())
                    .ok_or(MeasureError::MissingFrameData {
                        what: "dUT1 (UT1-UTC offset in seconds; set with_dut1() or with_eop())",
                    })?;
            // UT1 = UTC + dUT1
            Ok(value + dut1 / SECONDS_PER_DAY)
        }
        // UT1 → GMST1: use sofars gmst06
        (UT1, GMST1) => {
            // gmst06 needs both UT1 and TT as 2-part JDs.
            // Derive TT from UT1 via UT1→UTC→TAI→TT.
            let utc = apply_hop(value, UT1, UTC, frame)?;
            let tai = apply_hop(utc, UTC, TAI, frame)?;
            let tt = apply_hop(tai, TAI, TT, frame)?;

            let (ut_a, ut_b) = value.as_jd_pair();
            let (tt_a, tt_b) = tt.as_jd_pair();

            let gmst_rad = sofars::erst::gmst06(ut_a, ut_b, tt_a, tt_b);
            // GMST is in radians; convert to fraction of day (turns)
            let gmst_turns = gmst_rad / (2.0 * std::f64::consts::PI);
            // The MJD for GMST is the integer UT1 day + GMST fraction
            Ok(MjdHighPrec::new(value.day(), gmst_turns))
        }
        // GMST1 → UT1: iterative Newton-Raphson inverse
        // GMST is a monotonically increasing function of UT1 fraction within a day.
        // The sidereal/solar ratio is approximately 1.00273781191.
        (GMST1, UT1) => {
            // The UT1 integer day is the same as the GMST day.
            // We need to find the UT1 fraction that produces the given GMST fraction.
            let gmst_frac = value.frac();

            // Initial guess: UT1_frac ≈ GMST_frac / sidereal_ratio
            const SIDEREAL_RATIO: f64 = 1.002_737_811_91;
            let mut ut1_frac = gmst_frac / SIDEREAL_RATIO;

            // Newton-Raphson iteration
            for _ in 0..15 {
                let trial_ut1 = MjdHighPrec::new(value.day(), ut1_frac);
                // Compute GMST for this trial UT1
                let trial_gmst = apply_hop(trial_ut1, UT1, GMST1, frame)?;
                let trial_gmst_frac = trial_gmst.frac();

                // Residual (handle wrapping)
                let mut residual = gmst_frac - trial_gmst_frac;
                if residual > 0.5 {
                    residual -= 1.0;
                }
                if residual < -0.5 {
                    residual += 1.0;
                }

                if residual.abs() < 1e-15 {
                    break;
                }

                // Correct: dUT1 ≈ dGMST / sidereal_ratio
                ut1_frac += residual / SIDEREAL_RATIO;
            }

            Ok(MjdHighPrec::new(value.day(), ut1_frac))
        }
        // GMST1 → LMST: add longitude
        (GMST1, LMST) => {
            let pos = frame.position().ok_or(MeasureError::MissingFrameData {
                what: "position (for observatory longitude)",
            })?;
            let lon_rad = pos.longitude_rad();
            let lon_turns = lon_rad / (2.0 * std::f64::consts::PI);
            Ok(MjdHighPrec::new(value.day(), value.frac() + lon_turns))
        }
        // LMST → GMST1: subtract longitude
        (LMST, GMST1) => {
            let pos = frame.position().ok_or(MeasureError::MissingFrameData {
                what: "position (for observatory longitude)",
            })?;
            let lon_rad = pos.longitude_rad();
            let lon_turns = lon_rad / (2.0 * std::f64::consts::PI);
            Ok(MjdHighPrec::new(value.day(), value.frac() - lon_turns))
        }
        // UT1 → GAST: Greenwich Apparent Sidereal Time
        (UT1, GAST) => {
            let (ut_a, ut_b) = value.as_jd_pair();

            let gast_rad = match frame.iau_model() {
                super::frame::IauModel::Iau1976_1980 => sofars::erst::gst94(ut_a, ut_b),
                super::frame::IauModel::Iau2006_2000A => {
                    // Match C++ casacore: GMST82(UT1) + ee00a(TT)
                    let utc = apply_hop(value, UT1, UTC, frame)?;
                    let tai = apply_hop(utc, UTC, TAI, frame)?;
                    let tt = apply_hop(tai, TAI, TT, frame)?;
                    let (tt_a, tt_b) = tt.as_jd_pair();
                    let gmst = sofars::erst::gmst82(ut_a, ut_b);
                    let ee = sofars::erst::ee00a(tt_a, tt_b);
                    sofars::vm::anp(gmst + ee)
                }
            };
            let gast_turns = gast_rad / (2.0 * std::f64::consts::PI);
            Ok(MjdHighPrec::new(value.day(), gast_turns))
        }
        // GAST → UT1: iterative Newton-Raphson inverse (same approach as GMST1→UT1)
        (GAST, UT1) => {
            let gast_frac = value.frac();
            const SIDEREAL_RATIO: f64 = 1.002_737_811_91;
            let mut ut1_frac = gast_frac / SIDEREAL_RATIO;

            for _ in 0..15 {
                let trial_ut1 = MjdHighPrec::new(value.day(), ut1_frac);
                let trial_gast = apply_hop(trial_ut1, UT1, GAST, frame)?;
                let trial_gast_frac = trial_gast.frac();

                let mut residual = gast_frac - trial_gast_frac;
                if residual > 0.5 {
                    residual -= 1.0;
                }
                if residual < -0.5 {
                    residual += 1.0;
                }

                if residual.abs() < 1e-15 {
                    break;
                }
                ut1_frac += residual / SIDEREAL_RATIO;
            }

            Ok(MjdHighPrec::new(value.day(), ut1_frac))
        }
        // GAST → LAST: add longitude (same as GMST1→LMST)
        (GAST, LAST) => {
            let pos = frame.position().ok_or(MeasureError::MissingFrameData {
                what: "position (for observatory longitude)",
            })?;
            let lon_rad = pos.longitude_rad();
            let lon_turns = lon_rad / (2.0 * std::f64::consts::PI);
            Ok(MjdHighPrec::new(value.day(), value.frac() + lon_turns))
        }
        // LAST → GAST: subtract longitude
        (LAST, GAST) => {
            let pos = frame.position().ok_or(MeasureError::MissingFrameData {
                what: "position (for observatory longitude)",
            })?;
            let lon_rad = pos.longitude_rad();
            let lon_turns = lon_rad / (2.0 * std::f64::consts::PI);
            Ok(MjdHighPrec::new(value.day(), value.frac() - lon_turns))
        }
        _ => Err(MeasureError::NotYetImplemented {
            route: format!("{from_ref} → {to_ref}"),
        }),
    }
}

/// Computes the TDB−TT approximation (`dtr`) for the sofars `tttdb`/`tdbtt`
/// functions.
///
/// When a position is available in the frame, uses the observatory coordinates.
/// Otherwise, uses zero position (like C++ casacore does when no frame is set).
fn compute_dtdb(value: MjdHighPrec, frame: &MeasFrame) -> f64 {
    let (jd1, jd2) = value.as_jd_pair();
    let ut_frac = value.frac();

    let (elong, u_km, v_km) = if let Some(pos) = frame.position() {
        // Get ITRF coordinates for dtdb.
        let itrf = pos.as_itrf();
        let x = itrf[0];
        let y = itrf[1];
        let z = itrf[2];
        // Compute cylindrical coords: u = distance from spin axis, v = z
        let u = (x * x + y * y).sqrt() / 1000.0; // m → km
        let v = z / 1000.0; // m → km
        let elong = y.atan2(x);
        (elong, u, v)
    } else {
        (0.0, 0.0, 0.0)
    };

    sofars::ts::dtdb(jd1, jd2, ut_frac, elong, u_km, v_km)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mjd_high_prec_normalization() {
        let m = MjdHighPrec::new(51544.0, 1.5);
        assert_eq!(m.day(), 51545.0);
        assert!((m.frac() - 0.5).abs() < 1e-15);
    }

    #[test]
    fn mjd_high_prec_from_mjd_roundtrip() {
        let mjd = 51544.5;
        let m = MjdHighPrec::from_mjd(mjd);
        assert!((m.as_mjd() - mjd).abs() < 1e-15);
        assert_eq!(m.day(), 51544.0);
        assert!((m.frac() - 0.5).abs() < 1e-15);
    }

    #[test]
    fn mjd_high_prec_jd_pair_roundtrip() {
        let m = MjdHighPrec::new(51544.0, 0.5);
        let (jd1, jd2) = m.as_jd_pair();
        assert_eq!(jd1, MJD_OFFSET);
        assert!((jd2 - 51544.5).abs() < 1e-15);
        let m2 = MjdHighPrec::from_jd_pair(jd1, jd2);
        assert!((m2.as_mjd() - m.as_mjd()).abs() < 1e-15);
    }

    #[test]
    fn mjd_high_prec_arithmetic() {
        let m = MjdHighPrec::from_mjd(51544.5);
        let m2 = m + 1.0;
        assert!((m2.as_mjd() - 51545.5).abs() < 1e-15);
        let m3 = m2 - 1.0;
        assert!((m3.as_mjd() - 51544.5).abs() < 1e-15);
        let diff = m2 - m;
        assert!((diff - 1.0).abs() < 1e-15);
    }

    #[test]
    fn epoch_ref_parse_all() {
        for r in EpochRef::ALL {
            let parsed: EpochRef = r.as_str().parse().unwrap();
            assert_eq!(parsed, r);
        }
    }

    #[test]
    fn epoch_ref_parse_synonyms() {
        assert_eq!("IAT".parse::<EpochRef>().unwrap(), EpochRef::TAI);
        assert_eq!("TDT".parse::<EpochRef>().unwrap(), EpochRef::TT);
        assert_eq!("ET".parse::<EpochRef>().unwrap(), EpochRef::TT);
        assert_eq!("UT".parse::<EpochRef>().unwrap(), EpochRef::UT1);
        assert_eq!("GMST".parse::<EpochRef>().unwrap(), EpochRef::GMST1);
    }

    #[test]
    fn epoch_ref_unknown() {
        assert!("XYZZY".parse::<EpochRef>().is_err());
    }

    #[test]
    fn casacore_code_roundtrip() {
        for r in EpochRef::ALL {
            let code = r.casacore_code();
            assert_eq!(EpochRef::from_casacore_code(code), Some(r));
        }
    }

    #[test]
    fn casacore_code_known_values() {
        assert_eq!(EpochRef::UTC.casacore_code(), 6);
        assert_eq!(EpochRef::TAI.casacore_code(), 7);
        assert_eq!(EpochRef::TT.casacore_code(), 8); // C++ TDT=8
        assert_eq!(EpochRef::LAST.casacore_code(), 0);
        assert_eq!(EpochRef::TCB.casacore_code(), 11);
    }

    #[test]
    fn casacore_code_invalid() {
        assert_eq!(EpochRef::from_casacore_code(-1), None);
        assert_eq!(EpochRef::from_casacore_code(12), None);
    }
}
