// SPDX-License-Identifier: LGPL-3.0-or-later
//! Frequency measure: spectral frequencies in various velocity reference frames.
//!
//! This module provides:
//!
//! - [`FrequencyRef`] — velocity reference frame types (LSRK, BARY, TOPO, etc.).
//! - [`MFrequency`] — a frequency in a specified reference frame, equivalent
//!   to C++ `MFrequency`.
//!
//! Conversions between frames use relativistic Doppler shifts computed from
//! the radial velocity of one frame relative to another along the line of sight.
//!
//! # Velocity constants
//!
//! The standard velocity vectors (from casacore `MeasTable`) are defined in J2000
//! coordinates as direction × speed (m/s):
//!
//! | Frame | Speed | Reference |
//! |-------|-------|-----------|
//! | LSRK | 20 km/s | Solar motion, kinematic (IAU 1985) |
//! | LSRD | 16.55 km/s | Solar motion, dynamical (9,12,7 km/s galactic) |
//! | GALACTO | 220 km/s | Galactic rotation |
//! | LGROUP | 308 km/s | Local group motion |
//! | CMB | 369.5 km/s | CMB dipole |

use std::fmt;
use std::str::FromStr;

use super::epoch::EpochRef;
use super::error::MeasureError;
use super::frame::MeasFrame;

/// Speed of light in m/s.
pub(super) const C_M_PER_S: f64 = 299_792_458.0;

// ---------------------------------------------------------------------------
// FrequencyRef
// ---------------------------------------------------------------------------

/// Velocity reference frame types for frequency measures.
///
/// Corresponds to a subset of C++ `MFrequency::Types`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrequencyRef {
    /// Source rest frame. Conversions deferred (need radial velocity).
    REST,
    /// Local Standard of Rest, kinematic definition.
    LSRK,
    /// Local Standard of Rest, dynamical definition.
    LSRD,
    /// Solar system barycenter.
    BARY,
    /// Geocentric (Earth center).
    GEO,
    /// Topocentric (observatory location).
    TOPO,
    /// Galactocentric.
    GALACTO,
    /// Local group.
    LGROUP,
    /// Cosmic microwave background.
    CMB,
}

/// Total number of FrequencyRef variants.
const NUM_FREQ_REFS: usize = 9;

impl FrequencyRef {
    /// Returns the C++ casacore integer code for this reference type.
    ///
    /// These codes match the `MFrequency::Types` enum values defined in C++
    /// `MFrequency.h`.
    pub fn casacore_code(self) -> i32 {
        match self {
            Self::REST => 0,
            Self::LSRK => 1,
            Self::LSRD => 2,
            Self::BARY => 3,
            Self::GEO => 4,
            Self::TOPO => 5,
            Self::GALACTO => 6,
            Self::LGROUP => 7,
            Self::CMB => 8,
        }
    }

    /// Converts a C++ casacore integer code to the corresponding reference type.
    pub fn from_casacore_code(code: i32) -> Option<Self> {
        match code {
            0 => Some(Self::REST),
            1 => Some(Self::LSRK),
            2 => Some(Self::LSRD),
            3 => Some(Self::BARY),
            4 => Some(Self::GEO),
            5 => Some(Self::TOPO),
            6 => Some(Self::GALACTO),
            7 => Some(Self::LGROUP),
            8 => Some(Self::CMB),
            _ => None,
        }
    }

    /// All reference types in canonical order.
    pub const ALL: [FrequencyRef; NUM_FREQ_REFS] = [
        Self::REST,
        Self::LSRK,
        Self::LSRD,
        Self::BARY,
        Self::GEO,
        Self::TOPO,
        Self::GALACTO,
        Self::LGROUP,
        Self::CMB,
    ];

    /// Returns the canonical string name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::REST => "REST",
            Self::LSRK => "LSRK",
            Self::LSRD => "LSRD",
            Self::BARY => "BARY",
            Self::GEO => "GEO",
            Self::TOPO => "TOPO",
            Self::GALACTO => "GALACTO",
            Self::LGROUP => "LGROUP",
            Self::CMB => "CMB",
        }
    }
}

impl FromStr for FrequencyRef {
    type Err = MeasureError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "REST" => Ok(Self::REST),
            "LSRK" => Ok(Self::LSRK),
            "LSRD" => Ok(Self::LSRD),
            "BARY" | "BARYCENTRIC" => Ok(Self::BARY),
            "GEO" | "GEOCENTRIC" => Ok(Self::GEO),
            "TOPO" | "TOPOCENTRIC" => Ok(Self::TOPO),
            "GALACTO" | "GALACTOCENTRIC" => Ok(Self::GALACTO),
            "LGROUP" => Ok(Self::LGROUP),
            "CMB" => Ok(Self::CMB),
            _ => Err(MeasureError::UnknownRefType {
                input: s.to_owned(),
            }),
        }
    }
}

impl fmt::Display for FrequencyRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// MFrequency
// ---------------------------------------------------------------------------

/// A frequency in a specified velocity reference frame.
///
/// `MFrequency` stores a frequency in Hz with an associated reference frame.
/// This is the Rust equivalent of C++ `casa::MFrequency`.
///
/// # Conversions
///
/// Use [`convert_to`](MFrequency::convert_to) to transform between frames.
/// Most conversions require a direction in the [`MeasFrame`] (to compute the
/// radial velocity component). Some also need epoch and position.
///
/// # Examples
///
/// ```
/// use casacore_types::measures::frequency::{MFrequency, FrequencyRef};
/// use casacore_types::measures::direction::{MDirection, DirectionRef};
/// use casacore_types::measures::MeasFrame;
///
/// let freq = MFrequency::new(1.42e9, FrequencyRef::LSRK);
/// let dir = MDirection::from_angles(0.0, 0.0, DirectionRef::J2000);
/// let frame = MeasFrame::new().with_direction(dir);
/// let bary = freq.convert_to(FrequencyRef::BARY, &frame).unwrap();
/// // Frequency shift depends on direction relative to solar motion
/// assert!((bary.hz() - 1.42e9).abs() < 1e6); // within ~1 MHz
/// ```
#[derive(Debug, Clone)]
pub struct MFrequency {
    hz: f64,
    refer: FrequencyRef,
}

impl MFrequency {
    /// Creates a new frequency from a value in Hz and a reference frame.
    pub fn new(hz: f64, refer: FrequencyRef) -> Self {
        Self { hz, refer }
    }

    /// Returns the frequency in Hz.
    pub fn hz(&self) -> f64 {
        self.hz
    }

    /// Returns the reference frame.
    pub fn refer(&self) -> FrequencyRef {
        self.refer
    }

    /// Converts this frequency to a different reference frame.
    ///
    /// Most conversions require a direction in the frame (for velocity
    /// projection). Some also need epoch (BARY↔GEO) and position (GEO↔TOPO).
    pub fn convert_to(
        &self,
        target: FrequencyRef,
        frame: &MeasFrame,
    ) -> Result<MFrequency, MeasureError> {
        if self.refer == target {
            return Ok(self.clone());
        }
        let path = find_freq_path(self.refer, target)?;
        let mut hz = self.hz;
        let mut current_ref = self.refer;

        for next_ref in path {
            hz = apply_freq_hop(hz, current_ref, next_ref, frame)?;
            current_ref = next_ref;
        }

        Ok(MFrequency { hz, refer: target })
    }
}

impl fmt::Display for MFrequency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.hz.abs() >= 1e9 {
            write!(f, "Frequency: {:.6} GHz {}", self.hz / 1e9, self.refer)
        } else if self.hz.abs() >= 1e6 {
            write!(f, "Frequency: {:.6} MHz {}", self.hz / 1e6, self.refer)
        } else {
            write!(f, "Frequency: {:.3} Hz {}", self.hz, self.refer)
        }
    }
}

// ---------------------------------------------------------------------------
// Velocity constants from casacore MeasTable (which=0, J2000)
// ---------------------------------------------------------------------------

/// LSRK velocity: Solar motion relative to kinematic LSR.
/// Direction cosines (J2000) × speed (m/s): 20 km/s.
pub(super) const LSRK_VELOCITY: [f64; 3] = [
    20000.0 * 0.014_502_1,
    20000.0 * -0.865_863,
    20000.0 * 0.500_071,
];

/// LSRD velocity: Solar motion relative to dynamical LSR.
/// Direction cosines (J2000) × speed (m/s): sqrt(274) km/s ≈ 16552.9 m/s.
/// (9,12,7) km/s in galactic, converted to J2000.
pub(super) const LSRD_SPEED: f64 = 16_552.945_357_171_347; // sqrt(274) * 1000
pub(super) const LSRD_VELOCITY: [f64; 3] = [
    LSRD_SPEED * -0.038_556_8,
    LSRD_SPEED * -0.881_138,
    LSRD_SPEED * 0.471_285,
];

/// Galactocentric velocity: galactic rotation relative to LSR(D).
/// 220 km/s toward l=90°, b=0° (galactic), converted to J2000.
pub(super) const GALACTO_VELOCITY: [f64; 3] = [
    220000.0 * 0.494_109,
    220000.0 * -0.444_83,
    220000.0 * 0.746_982,
];

/// Local group velocity relative to barycenter.
/// 308 km/s toward l=105°, b=-7°, converted to J2000.
pub(super) const LGROUP_VELOCITY: [f64; 3] = [
    308000.0 * 0.593_553_979_227,
    308000.0 * -0.177_954_636_914,
    308000.0 * 0.784_873_124_106,
];

/// CMB dipole velocity relative to barycenter.
/// 369.5 km/s toward l=264.4°, b=48.4°, converted to J2000.
pub(super) const CMB_VELOCITY: [f64; 3] = [
    369500.0 * -0.971_769_852_57,
    369500.0 * 0.202_393_953_108,
    369500.0 * -0.121_243_727_187,
];

// ---------------------------------------------------------------------------
// Relativistic Doppler
// ---------------------------------------------------------------------------

/// Apply relativistic Doppler shift.
///
/// `beta` is the radial velocity in units of c (positive = approaching).
/// Returns `f_out = f_in * sqrt((1 + beta) / (1 - beta))`.
pub(super) fn doppler_shift(f_in: f64, beta: f64) -> f64 {
    f_in * ((1.0 + beta) / (1.0 - beta)).sqrt()
}

/// Compute the radial velocity beta = v · d̂ / c for a constant velocity vector.
pub(super) fn compute_beta(velocity_ms: &[f64; 3], dir_j2000: &[f64; 3]) -> f64 {
    sofars::vm::pdp(velocity_ms, dir_j2000) / C_M_PER_S
}

/// Get direction in J2000 from the frame.
pub(super) fn get_direction_j2000(frame: &MeasFrame) -> Result<[f64; 3], MeasureError> {
    let dir = frame.direction().ok_or(MeasureError::MissingFrameData {
        what: "direction (for velocity projection)",
    })?;
    dir.to_j2000(frame)
}

/// Get Earth's barycentric velocity in m/s (J2000).
pub(super) fn earth_velocity_ms(frame: &MeasFrame) -> Result<[f64; 3], MeasureError> {
    let epoch = frame.epoch().ok_or(MeasureError::MissingFrameData {
        what: "epoch (for Earth orbital velocity)",
    })?;
    let tt = if epoch.refer() == EpochRef::TT {
        epoch.clone()
    } else {
        epoch.convert_to(EpochRef::TT, frame)?
    };
    let (tt1, tt2) = tt.value().as_jd_pair();
    let (_pvh, pvb) = sofars::eph::epv00(tt1, tt2).ok_or(MeasureError::SofarsError { code: -1 })?;
    // pvb[1] is Earth barycentric velocity in AU/day → convert to m/s
    let au_to_m = 149_597_870_700.0_f64;
    let day_to_s = 86400.0;
    Ok([
        pvb[1][0] * au_to_m / day_to_s,
        pvb[1][1] * au_to_m / day_to_s,
        pvb[1][2] * au_to_m / day_to_s,
    ])
}

/// Get observatory velocity in m/s (J2000) from Earth rotation.
pub(super) fn observatory_velocity_ms(frame: &MeasFrame) -> Result<[f64; 3], MeasureError> {
    let pos = frame.position().ok_or(MeasureError::MissingFrameData {
        what: "position (for diurnal velocity)",
    })?;
    let epoch = frame.epoch().ok_or(MeasureError::MissingFrameData {
        what: "epoch (for Earth rotation angle)",
    })?;

    // Get ITRF coordinates
    let itrf = pos.as_itrf();

    // Earth angular velocity (rad/s)
    const OMEGA: f64 = 7.292_115e-5;

    // Velocity in ITRF: v = omega cross r = [-omega*y, omega*x, 0]
    let v_itrf = [-OMEGA * itrf[1], OMEGA * itrf[0], 0.0];

    // Rotate to J2000 using ERA (simplified — ignores precession/nutation)
    let ut1 = if epoch.refer() == EpochRef::UT1 {
        epoch.clone()
    } else {
        epoch.convert_to(EpochRef::UT1, frame)?
    };
    let (ut_a, ut_b) = ut1.value().as_jd_pair();
    let era = sofars::erst::era00(ut_a, ut_b);

    // Rotate velocity from ITRF to J2000 by -ERA around z
    let mut r = [[0.0; 3]; 3];
    sofars::vm::ir(&mut r);
    sofars::vm::rz(-era, &mut r);
    let mut v_j2000 = [0.0; 3];
    sofars::vm::rxp(&r, &v_itrf, &mut v_j2000);

    Ok(v_j2000)
}

// ---------------------------------------------------------------------------
// Frequency routing graph
// ---------------------------------------------------------------------------

const FREQ_ROUTING_EDGES: &[(FrequencyRef, FrequencyRef)] = &[
    (FrequencyRef::REST, FrequencyRef::LSRK),
    (FrequencyRef::LSRK, FrequencyRef::BARY),
    (FrequencyRef::LSRD, FrequencyRef::BARY),
    (FrequencyRef::LSRD, FrequencyRef::GALACTO),
    (FrequencyRef::BARY, FrequencyRef::LGROUP),
    (FrequencyRef::BARY, FrequencyRef::CMB),
    (FrequencyRef::BARY, FrequencyRef::GEO),
    (FrequencyRef::GEO, FrequencyRef::TOPO),
];

fn freq_ref_index(r: FrequencyRef) -> usize {
    match r {
        FrequencyRef::REST => 0,
        FrequencyRef::LSRK => 1,
        FrequencyRef::LSRD => 2,
        FrequencyRef::BARY => 3,
        FrequencyRef::GEO => 4,
        FrequencyRef::TOPO => 5,
        FrequencyRef::GALACTO => 6,
        FrequencyRef::LGROUP => 7,
        FrequencyRef::CMB => 8,
    }
}

fn find_freq_path(
    source: FrequencyRef,
    target: FrequencyRef,
) -> Result<Vec<FrequencyRef>, MeasureError> {
    use std::collections::VecDeque;

    let mut visited = [false; NUM_FREQ_REFS];
    let mut parent: [Option<FrequencyRef>; NUM_FREQ_REFS] = [None; NUM_FREQ_REFS];
    let mut queue = VecDeque::new();

    visited[freq_ref_index(source)] = true;
    queue.push_back(source);

    let mut found = false;

    while let Some(current) = queue.pop_front() {
        if current == target {
            found = true;
            break;
        }

        for &(a, b) in FREQ_ROUTING_EDGES {
            let neighbor = if a == current {
                b
            } else if b == current {
                a
            } else {
                continue;
            };
            let idx = freq_ref_index(neighbor);
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
            cur = parent[freq_ref_index(cur)].expect("BFS parent must be set");
        }
        path.reverse();
        return Ok(path);
    }

    Err(MeasureError::NotYetImplemented {
        route: format!("{source} → {target} (no route found)"),
    })
}

// ---------------------------------------------------------------------------
// Frequency hop implementations
// ---------------------------------------------------------------------------

fn apply_freq_hop(
    hz: f64,
    from: FrequencyRef,
    to: FrequencyRef,
    frame: &MeasFrame,
) -> Result<f64, MeasureError> {
    use FrequencyRef::*;

    match (from, to) {
        // REST ↔ LSRK: requires radial velocity in frame
        // The REST frame is the source rest frame. To convert REST → LSRK,
        // we need the radial velocity of the source in the LSRK frame.
        // f_lsrk = f_rest * sqrt((1 - v/c) / (1 + v/c))  [source receding → v positive → redshift]
        (REST, LSRK) => {
            let rv = frame
                .radial_velocity()
                .ok_or(MeasureError::MissingFrameData {
                    what: "radial_velocity (for REST frequency conversion)",
                })?;
            // Get the radial velocity in LSRK m/s
            let rv_lsrk = rv.convert_to(super::radial_velocity::RadialVelocityRef::LSRK, frame)?;
            let beta = rv_lsrk.ms() / C_M_PER_S;
            // Source receding (positive velocity) → frequency decreases → negative beta for doppler_shift
            Ok(doppler_shift(hz, -beta))
        }
        (LSRK, REST) => {
            let rv = frame
                .radial_velocity()
                .ok_or(MeasureError::MissingFrameData {
                    what: "radial_velocity (for REST frequency conversion)",
                })?;
            let rv_lsrk = rv.convert_to(super::radial_velocity::RadialVelocityRef::LSRK, frame)?;
            let beta = rv_lsrk.ms() / C_M_PER_S;
            Ok(doppler_shift(hz, beta))
        }

        // LSRK ↔ BARY: 20 km/s solar motion
        (LSRK, BARY) => {
            let d = get_direction_j2000(frame)?;
            let beta = compute_beta(&LSRK_VELOCITY, &d);
            Ok(doppler_shift(hz, beta))
        }
        (BARY, LSRK) => {
            let d = get_direction_j2000(frame)?;
            let beta = compute_beta(&LSRK_VELOCITY, &d);
            Ok(doppler_shift(hz, -beta))
        }

        // LSRD ↔ BARY: dynamical solar motion
        (LSRD, BARY) => {
            let d = get_direction_j2000(frame)?;
            let beta = compute_beta(&LSRD_VELOCITY, &d);
            Ok(doppler_shift(hz, beta))
        }
        (BARY, LSRD) => {
            let d = get_direction_j2000(frame)?;
            let beta = compute_beta(&LSRD_VELOCITY, &d);
            Ok(doppler_shift(hz, -beta))
        }

        // LSRD ↔ GALACTO: galactic rotation
        (LSRD, GALACTO) => {
            let d = get_direction_j2000(frame)?;
            let beta = compute_beta(&GALACTO_VELOCITY, &d);
            Ok(doppler_shift(hz, -beta))
        }
        (GALACTO, LSRD) => {
            let d = get_direction_j2000(frame)?;
            let beta = compute_beta(&GALACTO_VELOCITY, &d);
            Ok(doppler_shift(hz, beta))
        }

        // BARY ↔ LGROUP
        (LGROUP, BARY) => {
            let d = get_direction_j2000(frame)?;
            let beta = compute_beta(&LGROUP_VELOCITY, &d);
            Ok(doppler_shift(hz, beta))
        }
        (BARY, LGROUP) => {
            let d = get_direction_j2000(frame)?;
            let beta = compute_beta(&LGROUP_VELOCITY, &d);
            Ok(doppler_shift(hz, -beta))
        }

        // BARY ↔ CMB
        (CMB, BARY) => {
            let d = get_direction_j2000(frame)?;
            let beta = compute_beta(&CMB_VELOCITY, &d);
            Ok(doppler_shift(hz, beta))
        }
        (BARY, CMB) => {
            let d = get_direction_j2000(frame)?;
            let beta = compute_beta(&CMB_VELOCITY, &d);
            Ok(doppler_shift(hz, -beta))
        }

        // BARY ↔ GEO: Earth orbital velocity (epoch-dependent)
        (BARY, GEO) => {
            let d = get_direction_j2000(frame)?;
            let v_earth = earth_velocity_ms(frame)?;
            let beta = compute_beta(&v_earth, &d);
            Ok(doppler_shift(hz, beta))
        }
        (GEO, BARY) => {
            let d = get_direction_j2000(frame)?;
            let v_earth = earth_velocity_ms(frame)?;
            let beta = compute_beta(&v_earth, &d);
            Ok(doppler_shift(hz, -beta))
        }

        // GEO ↔ TOPO: diurnal rotation velocity
        (GEO, TOPO) => {
            let d = get_direction_j2000(frame)?;
            let v_obs = observatory_velocity_ms(frame)?;
            let beta = compute_beta(&v_obs, &d);
            Ok(doppler_shift(hz, beta))
        }
        (TOPO, GEO) => {
            let d = get_direction_j2000(frame)?;
            let v_obs = observatory_velocity_ms(frame)?;
            let beta = compute_beta(&v_obs, &d);
            Ok(doppler_shift(hz, -beta))
        }

        _ => Err(MeasureError::NotYetImplemented {
            route: format!("{from} → {to}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::measures::direction::{DirectionRef, MDirection};

    #[test]
    fn frequency_ref_parse_all() {
        for r in FrequencyRef::ALL {
            let parsed: FrequencyRef = r.as_str().parse().unwrap();
            assert_eq!(parsed, r);
        }
    }

    #[test]
    fn lsrk_to_bary_needs_direction() {
        let f = MFrequency::new(1.42e9, FrequencyRef::LSRK);
        let frame = MeasFrame::new(); // no direction
        let result = f.convert_to(FrequencyRef::BARY, &frame);
        assert!(matches!(result, Err(MeasureError::MissingFrameData { .. })));
    }

    #[test]
    fn lsrk_to_bary_roundtrip() {
        let f = MFrequency::new(1.42e9, FrequencyRef::LSRK);
        let dir = MDirection::from_angles(0.0, 0.5, DirectionRef::J2000);
        let frame = MeasFrame::new().with_direction(dir);

        let bary = f.convert_to(FrequencyRef::BARY, &frame).unwrap();
        let back = bary.convert_to(FrequencyRef::LSRK, &frame).unwrap();

        let diff = (back.hz() - f.hz()).abs();
        assert!(diff < 0.01, "Roundtrip error: {diff} Hz");
    }

    #[test]
    fn rest_needs_radial_velocity() {
        let f = MFrequency::new(1.42e9, FrequencyRef::REST);
        let dir = MDirection::from_angles(0.0, 0.5, DirectionRef::J2000);
        let frame = MeasFrame::new().with_direction(dir);
        let result = f.convert_to(FrequencyRef::LSRK, &frame);
        assert!(matches!(result, Err(MeasureError::MissingFrameData { .. })));
    }

    #[test]
    fn rest_to_lsrk_with_radial_velocity() {
        use crate::measures::radial_velocity::{MRadialVelocity, RadialVelocityRef};
        // Source at 1000 km/s in LSRK → should see a redshift
        let rv = MRadialVelocity::new(1_000_000.0, RadialVelocityRef::LSRK);
        let dir = MDirection::from_angles(0.0, 0.5, DirectionRef::J2000);
        let frame = MeasFrame::new()
            .with_direction(dir)
            .with_radial_velocity(rv);
        let f_rest = MFrequency::new(1.42e9, FrequencyRef::REST);
        let f_lsrk = f_rest.convert_to(FrequencyRef::LSRK, &frame).unwrap();
        // Receding source → observed frequency should be lower
        assert!(f_lsrk.hz() < f_rest.hz());
        // Roundtrip
        let back = f_lsrk.convert_to(FrequencyRef::REST, &frame).unwrap();
        assert!((back.hz() - f_rest.hz()).abs() < 0.1);
    }

    #[test]
    fn identity_conversion() {
        let f = MFrequency::new(1.42e9, FrequencyRef::LSRK);
        let frame = MeasFrame::new();
        let same = f.convert_to(FrequencyRef::LSRK, &frame).unwrap();
        assert_eq!(same.hz(), f.hz());
    }

    #[test]
    fn lsrk_to_bary_reasonable_shift() {
        // The LSRK velocity is 20 km/s. Maximum Doppler shift at 1.4 GHz
        // is about 20/3e5 * 1.4e9 ≈ 93 kHz.
        let f = MFrequency::new(1.4e9, FrequencyRef::LSRK);
        let dir = MDirection::from_angles(0.0, 0.0, DirectionRef::J2000);
        let frame = MeasFrame::new().with_direction(dir);

        let bary = f.convert_to(FrequencyRef::BARY, &frame).unwrap();
        let shift = (bary.hz() - f.hz()).abs();
        // Should be < 20 km/s / c * 1.4 GHz ≈ 93 kHz
        assert!(shift < 100_000.0, "shift = {shift} Hz, expected < 100 kHz");
    }

    #[test]
    fn casacore_code_roundtrip() {
        for r in FrequencyRef::ALL {
            let code = r.casacore_code();
            assert_eq!(FrequencyRef::from_casacore_code(code), Some(r));
        }
    }

    #[test]
    fn casacore_code_known_values() {
        assert_eq!(FrequencyRef::REST.casacore_code(), 0);
        assert_eq!(FrequencyRef::LSRK.casacore_code(), 1);
        assert_eq!(FrequencyRef::TOPO.casacore_code(), 5);
        assert_eq!(FrequencyRef::CMB.casacore_code(), 8);
    }
}
