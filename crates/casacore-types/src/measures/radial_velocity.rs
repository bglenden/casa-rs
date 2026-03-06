// SPDX-License-Identifier: LGPL-3.0-or-later
//! Radial velocity measure: line-of-sight velocities in various reference frames.
//!
//! This module provides:
//!
//! - [`RadialVelocityRef`] — velocity reference frame types (LSRK, BARY, GEO, etc.).
//! - [`MRadialVelocity`] — a radial velocity in a specified reference frame,
//!   equivalent to C++ `MRadialVelocity`.
//!
//! Conversions use the same velocity constants and Doppler algebra as
//! [`MFrequency`](super::frequency::MFrequency), internally converting
//! velocity → frequency → shift → velocity for numerical agreement with C++.
//!
//! # Conversion approach
//!
//! C++ casacore converts radial velocities by:
//! 1. Convert v → frequency using f = f₀ · √((1−β)/(1+β)) where β = v/c
//! 2. Shift frequency between frames
//! 3. Convert back to velocity: v = c · (1 − (f/f₀)²) / (1 + (f/f₀)²)
//!
//! We match this approach for numerical compatibility.

use std::collections::VecDeque;
use std::fmt;
use std::str::FromStr;

use super::error::MeasureError;
use super::frame::MeasFrame;
use super::frequency::{
    C_M_PER_S, CMB_VELOCITY, GALACTO_VELOCITY, LGROUP_VELOCITY, LSRD_VELOCITY, LSRK_VELOCITY,
    compute_beta, doppler_shift, earth_velocity_ms, get_direction_j2000, observatory_velocity_ms,
};

// ---------------------------------------------------------------------------
// RadialVelocityRef
// ---------------------------------------------------------------------------

/// Velocity reference frame types for radial velocity measures.
///
/// Corresponds to C++ `MRadialVelocity::Types`. These are the same frames
/// as [`FrequencyRef`](super::frequency::FrequencyRef) minus REST.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RadialVelocityRef {
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

/// Total number of RadialVelocityRef variants.
const NUM_RV_REFS: usize = 8;

impl RadialVelocityRef {
    /// Returns the C++ casacore integer code for this reference type.
    ///
    /// These codes match the `MRadialVelocity::Types` enum values defined in
    /// C++ `MRadialVelocity.h`.
    pub fn casacore_code(self) -> i32 {
        match self {
            Self::LSRK => 0,
            Self::LSRD => 1,
            Self::BARY => 2,
            Self::GEO => 3,
            Self::TOPO => 4,
            Self::GALACTO => 5,
            Self::LGROUP => 6,
            Self::CMB => 7,
        }
    }

    /// Converts a C++ casacore integer code to the corresponding reference type.
    pub fn from_casacore_code(code: i32) -> Option<Self> {
        match code {
            0 => Some(Self::LSRK),
            1 => Some(Self::LSRD),
            2 => Some(Self::BARY),
            3 => Some(Self::GEO),
            4 => Some(Self::TOPO),
            5 => Some(Self::GALACTO),
            6 => Some(Self::LGROUP),
            7 => Some(Self::CMB),
            _ => None,
        }
    }

    /// All reference types in canonical order.
    pub const ALL: [RadialVelocityRef; NUM_RV_REFS] = [
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

impl FromStr for RadialVelocityRef {
    type Err = MeasureError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
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

impl fmt::Display for RadialVelocityRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// MRadialVelocity
// ---------------------------------------------------------------------------

/// A radial velocity in a specified reference frame.
///
/// `MRadialVelocity` stores a velocity in m/s with an associated reference frame.
/// This is the Rust equivalent of C++ `casa::MRadialVelocity`.
///
/// # Conversions
///
/// Use [`convert_to`](MRadialVelocity::convert_to) to transform between frames.
/// Most conversions require a direction in the [`MeasFrame`] (for velocity
/// projection). Some also need epoch and position.
///
/// # Examples
///
/// ```
/// use casacore_types::measures::radial_velocity::{MRadialVelocity, RadialVelocityRef};
/// use casacore_types::measures::direction::{MDirection, DirectionRef};
/// use casacore_types::measures::MeasFrame;
///
/// let rv = MRadialVelocity::new(100_000.0, RadialVelocityRef::LSRK); // 100 km/s
/// let dir = MDirection::from_angles(0.0, 0.0, DirectionRef::J2000);
/// let frame = MeasFrame::new().with_direction(dir);
/// let bary = rv.convert_to(RadialVelocityRef::BARY, &frame).unwrap();
/// // Velocity shift depends on direction relative to solar motion
/// assert!((bary.ms() - 100_000.0).abs() < 25_000.0);
/// ```
#[derive(Debug, Clone)]
pub struct MRadialVelocity {
    ms: f64,
    refer: RadialVelocityRef,
}

impl MRadialVelocity {
    /// Creates a new radial velocity from a value in m/s and a reference frame.
    pub fn new(ms: f64, refer: RadialVelocityRef) -> Self {
        Self { ms, refer }
    }

    /// Returns the velocity in m/s.
    pub fn ms(&self) -> f64 {
        self.ms
    }

    /// Returns the reference frame.
    pub fn refer(&self) -> RadialVelocityRef {
        self.refer
    }

    /// Converts this radial velocity to a different reference frame.
    ///
    /// The conversion uses relativistic Doppler shifts internally:
    /// v → frequency → shift between frames → v, matching C++ casacore's approach.
    pub fn convert_to(
        &self,
        target: RadialVelocityRef,
        frame: &MeasFrame,
    ) -> Result<MRadialVelocity, MeasureError> {
        if self.refer == target {
            return Ok(self.clone());
        }
        let path = find_rv_path(self.refer, target)?;
        let mut ms = self.ms;
        let mut current_ref = self.refer;

        for next_ref in path {
            ms = apply_rv_hop(ms, current_ref, next_ref, frame)?;
            current_ref = next_ref;
        }

        Ok(MRadialVelocity { ms, refer: target })
    }
}

impl fmt::Display for MRadialVelocity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.ms.abs() >= 1000.0 {
            write!(
                f,
                "RadialVelocity: {:.3} km/s {}",
                self.ms / 1000.0,
                self.refer
            )
        } else {
            write!(f, "RadialVelocity: {:.3} m/s {}", self.ms, self.refer)
        }
    }
}

// ---------------------------------------------------------------------------
// Routing graph (same topology as frequency)
// ---------------------------------------------------------------------------

const RV_ROUTING_EDGES: &[(RadialVelocityRef, RadialVelocityRef)] = &[
    (RadialVelocityRef::LSRK, RadialVelocityRef::BARY),
    (RadialVelocityRef::LSRD, RadialVelocityRef::BARY),
    (RadialVelocityRef::LSRD, RadialVelocityRef::GALACTO),
    (RadialVelocityRef::BARY, RadialVelocityRef::LGROUP),
    (RadialVelocityRef::BARY, RadialVelocityRef::CMB),
    (RadialVelocityRef::BARY, RadialVelocityRef::GEO),
    (RadialVelocityRef::GEO, RadialVelocityRef::TOPO),
];

fn rv_ref_index(r: RadialVelocityRef) -> usize {
    match r {
        RadialVelocityRef::LSRK => 0,
        RadialVelocityRef::LSRD => 1,
        RadialVelocityRef::BARY => 2,
        RadialVelocityRef::GEO => 3,
        RadialVelocityRef::TOPO => 4,
        RadialVelocityRef::GALACTO => 5,
        RadialVelocityRef::LGROUP => 6,
        RadialVelocityRef::CMB => 7,
    }
}

fn find_rv_path(
    source: RadialVelocityRef,
    target: RadialVelocityRef,
) -> Result<Vec<RadialVelocityRef>, MeasureError> {
    let mut visited = [false; NUM_RV_REFS];
    let mut parent: [Option<RadialVelocityRef>; NUM_RV_REFS] = [None; NUM_RV_REFS];
    let mut queue = VecDeque::new();

    visited[rv_ref_index(source)] = true;
    queue.push_back(source);

    let mut found = false;

    while let Some(current) = queue.pop_front() {
        if current == target {
            found = true;
            break;
        }

        for &(a, b) in RV_ROUTING_EDGES {
            let neighbor = if a == current {
                b
            } else if b == current {
                a
            } else {
                continue;
            };
            let idx = rv_ref_index(neighbor);
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
            cur = parent[rv_ref_index(cur)].expect("BFS parent must be set");
        }
        path.reverse();
        return Ok(path);
    }

    Err(MeasureError::NotYetImplemented {
        route: format!("{source} → {target} (no route found)"),
    })
}

// ---------------------------------------------------------------------------
// Hop implementations
// ---------------------------------------------------------------------------

/// Convert velocity between frames using relativistic Doppler.
///
/// For each hop, we convert v → frequency (using a reference frequency f₀),
/// apply the frame velocity shift, then convert back to velocity.
/// This matches C++ casacore's `MCRadialVelocity` approach.
fn apply_rv_hop(
    ms: f64,
    from: RadialVelocityRef,
    to: RadialVelocityRef,
    frame: &MeasFrame,
) -> Result<f64, MeasureError> {
    use RadialVelocityRef::*;

    // Use a reference frequency for the v→f→v conversion.
    // The choice doesn't affect the result for relativistic Doppler.
    const F_REF: f64 = 1e9; // 1 GHz

    // Convert input velocity to frequency: f_in = f_ref * sqrt((1-β)/(1+β))
    let beta_in = ms / C_M_PER_S;
    let f_in = F_REF * ((1.0 - beta_in) / (1.0 + beta_in)).sqrt();

    // Apply the frequency shift for this hop
    let f_out = match (from, to) {
        // LSRK ↔ BARY
        (LSRK, BARY) => {
            let d = get_direction_j2000(frame)?;
            let beta = compute_beta(&LSRK_VELOCITY, &d);
            doppler_shift(f_in, beta)
        }
        (BARY, LSRK) => {
            let d = get_direction_j2000(frame)?;
            let beta = compute_beta(&LSRK_VELOCITY, &d);
            doppler_shift(f_in, -beta)
        }

        // LSRD ↔ BARY
        (LSRD, BARY) => {
            let d = get_direction_j2000(frame)?;
            let beta = compute_beta(&LSRD_VELOCITY, &d);
            doppler_shift(f_in, beta)
        }
        (BARY, LSRD) => {
            let d = get_direction_j2000(frame)?;
            let beta = compute_beta(&LSRD_VELOCITY, &d);
            doppler_shift(f_in, -beta)
        }

        // LSRD ↔ GALACTO
        (LSRD, GALACTO) => {
            let d = get_direction_j2000(frame)?;
            let beta = compute_beta(&GALACTO_VELOCITY, &d);
            doppler_shift(f_in, -beta)
        }
        (GALACTO, LSRD) => {
            let d = get_direction_j2000(frame)?;
            let beta = compute_beta(&GALACTO_VELOCITY, &d);
            doppler_shift(f_in, beta)
        }

        // BARY ↔ LGROUP
        (LGROUP, BARY) => {
            let d = get_direction_j2000(frame)?;
            let beta = compute_beta(&LGROUP_VELOCITY, &d);
            doppler_shift(f_in, beta)
        }
        (BARY, LGROUP) => {
            let d = get_direction_j2000(frame)?;
            let beta = compute_beta(&LGROUP_VELOCITY, &d);
            doppler_shift(f_in, -beta)
        }

        // BARY ↔ CMB
        (CMB, BARY) => {
            let d = get_direction_j2000(frame)?;
            let beta = compute_beta(&CMB_VELOCITY, &d);
            doppler_shift(f_in, beta)
        }
        (BARY, CMB) => {
            let d = get_direction_j2000(frame)?;
            let beta = compute_beta(&CMB_VELOCITY, &d);
            doppler_shift(f_in, -beta)
        }

        // BARY ↔ GEO
        (BARY, GEO) => {
            let d = get_direction_j2000(frame)?;
            let v_earth = earth_velocity_ms(frame)?;
            let beta = compute_beta(&v_earth, &d);
            doppler_shift(f_in, beta)
        }
        (GEO, BARY) => {
            let d = get_direction_j2000(frame)?;
            let v_earth = earth_velocity_ms(frame)?;
            let beta = compute_beta(&v_earth, &d);
            doppler_shift(f_in, -beta)
        }

        // GEO ↔ TOPO
        (GEO, TOPO) => {
            let d = get_direction_j2000(frame)?;
            let v_obs = observatory_velocity_ms(frame)?;
            let beta = compute_beta(&v_obs, &d);
            doppler_shift(f_in, beta)
        }
        (TOPO, GEO) => {
            let d = get_direction_j2000(frame)?;
            let v_obs = observatory_velocity_ms(frame)?;
            let beta = compute_beta(&v_obs, &d);
            doppler_shift(f_in, -beta)
        }

        _ => {
            return Err(MeasureError::NotYetImplemented {
                route: format!("{from} → {to}"),
            });
        }
    };

    // Convert back to velocity: β_out from f_out/f_ref ratio
    let ratio = f_out / F_REF;
    let r2 = ratio * ratio;
    let beta_out = (1.0 - r2) / (1.0 + r2);
    Ok(beta_out * C_M_PER_S)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::measures::direction::{DirectionRef, MDirection};

    #[test]
    fn radial_velocity_ref_parse_all() {
        for r in RadialVelocityRef::ALL {
            let parsed: RadialVelocityRef = r.as_str().parse().unwrap();
            assert_eq!(parsed, r);
        }
    }

    #[test]
    fn lsrk_to_bary_needs_direction() {
        let rv = MRadialVelocity::new(100_000.0, RadialVelocityRef::LSRK);
        let frame = MeasFrame::new();
        let result = rv.convert_to(RadialVelocityRef::BARY, &frame);
        assert!(matches!(result, Err(MeasureError::MissingFrameData { .. })));
    }

    #[test]
    fn lsrk_to_bary_roundtrip() {
        let rv = MRadialVelocity::new(100_000.0, RadialVelocityRef::LSRK);
        let dir = MDirection::from_angles(0.0, 0.5, DirectionRef::J2000);
        let frame = MeasFrame::new().with_direction(dir);

        let bary = rv.convert_to(RadialVelocityRef::BARY, &frame).unwrap();
        let back = bary.convert_to(RadialVelocityRef::LSRK, &frame).unwrap();

        let diff = (back.ms() - rv.ms()).abs();
        assert!(diff < 0.01, "Roundtrip error: {diff} m/s");
    }

    #[test]
    fn identity_conversion() {
        let rv = MRadialVelocity::new(50_000.0, RadialVelocityRef::LSRK);
        let frame = MeasFrame::new();
        let same = rv.convert_to(RadialVelocityRef::LSRK, &frame).unwrap();
        assert_eq!(same.ms(), rv.ms());
    }

    #[test]
    fn lsrk_to_bary_reasonable_shift() {
        // LSRK velocity is 20 km/s, so max shift in radial velocity is ~20 km/s
        let rv = MRadialVelocity::new(0.0, RadialVelocityRef::LSRK);
        let dir = MDirection::from_angles(0.0, 0.0, DirectionRef::J2000);
        let frame = MeasFrame::new().with_direction(dir);

        let bary = rv.convert_to(RadialVelocityRef::BARY, &frame).unwrap();
        assert!(
            bary.ms().abs() < 25_000.0,
            "shift = {} m/s, expected < 25 km/s",
            bary.ms()
        );
    }

    #[test]
    fn casacore_code_roundtrip() {
        for r in RadialVelocityRef::ALL {
            let code = r.casacore_code();
            assert_eq!(RadialVelocityRef::from_casacore_code(code), Some(r));
        }
    }

    #[test]
    fn casacore_code_known_values() {
        assert_eq!(RadialVelocityRef::LSRK.casacore_code(), 0);
        assert_eq!(RadialVelocityRef::BARY.casacore_code(), 2);
        assert_eq!(RadialVelocityRef::TOPO.casacore_code(), 4);
        assert_eq!(RadialVelocityRef::CMB.casacore_code(), 7);
    }
}
