// SPDX-License-Identifier: LGPL-3.0-or-later
//! Antennas shadowed by their neighbours, from projected baselines.
//!
//! Two dishes whose apertures overlap as seen from the source block each
//! other's view: the one behind, farther from the source, is shadowed. The
//! blocked fraction of each aperture follows casacore's
//! `NewMSSimulator::blockage`, from the projected separation `√(u²+v²)` and
//! the two diameters; an antenna is shadowed when the fraction exceeds a
//! limit.
//!
//! With casacore's uvw convention, a row's uvw is `R·(X₁ − X₂)`, so `w > 0`
//! puts antenna 1 nearer the source and antenna 2 behind it. CASA's
//! simulator (`NewMSSimulator.cc` 1812-1815) assumes the opposite sign and
//! charges the blockage to the antenna in front. CASA's own flagger
//! (`FlagAgentShadow`, since casapy 4.1) charges it to the one behind.
//! [`ShadowedAntenna`] chooses between them.

/// The blocked-aperture fraction above which CASA's simulator flags an
/// antenna (`setlimits(shadowlimit=...)`; casacore's default).
pub const CASA_SHADOW_FRACTION_LIMIT: f64 = 1.0e-6;

/// Which antenna of an overlapping pair a baseline's blockage is charged to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShadowedAntenna {
    /// The antenna farther from the source, which the other blocks.
    Behind,
    /// The antenna nearer the source, as CASA's simulator charges it.
    CasaSimulatorFront,
}

/// One projected baseline: its two antennas, its uvw in metres (casacore's
/// convention, `R·(X₁ − X₂)`) and the two dish diameters in metres.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProjectedBaseline {
    /// First antenna index.
    pub antenna1: usize,
    /// Second antenna index.
    pub antenna2: usize,
    /// The baseline's uvw, metres.
    pub uvw_m: [f64; 3],
    /// First antenna's dish diameter, metres.
    pub diameter1_m: f64,
    /// Second antenna's dish diameter, metres.
    pub diameter2_m: f64,
}

/// Which of `antenna_count` antennas are shadowed by any of `baselines`:
/// those whose blocked fraction exceeds `fraction_limit` on some baseline,
/// charged as `charged` says. Autocorrelations and out-of-range indices are
/// ignored.
pub fn shadowed_antennas(
    antenna_count: usize,
    baselines: impl IntoIterator<Item = ProjectedBaseline>,
    fraction_limit: f64,
    charged: ShadowedAntenna,
) -> Vec<bool> {
    let mut shadowed = vec![false; antenna_count];
    for baseline in baselines {
        let ProjectedBaseline {
            antenna1,
            antenna2,
            uvw_m,
            diameter1_m,
            diameter2_m,
        } = baseline;
        if antenna1 == antenna2 || antenna1 >= antenna_count || antenna2 >= antenna_count {
            continue;
        }
        let (fraction1, fraction2) = blockage_fractions(uvw_m, diameter1_m, diameter2_m, charged);
        if fraction1 > fraction_limit {
            shadowed[antenna1] = true;
        }
        if fraction2 > fraction_limit {
            shadowed[antenna2] = true;
        }
    }
    shadowed
}

/// The fractions of antenna 1's and antenna 2's apertures blocked on one
/// baseline, with the blockage charged to one antenna by the sign of w.
fn blockage_fractions(
    uvw_m: [f64; 3],
    diameter1_m: f64,
    diameter2_m: f64,
    charged: ShadowedAntenna,
) -> (f64, f64) {
    let separation = (uvw_m[0] * uvw_m[0] + uvw_m[1] * uvw_m[1]).sqrt();
    let diameter1 = diameter1_m.abs();
    let diameter2 = diameter2_m.abs();
    let rmin = 0.5 * diameter1.min(diameter2);
    let rmax = 0.5 * diameter1.max(diameter2);
    let (mut fraction1, mut fraction2) = if separation >= rmin + rmax {
        (0.0, 0.0)
    } else if separation + rmin <= rmax {
        (
            1.0_f64.min((diameter2 / diameter1).powi(2)),
            1.0_f64.min((diameter1 / diameter2).powi(2)),
        )
    } else {
        let c = separation / (0.5 * diameter1);
        let s = diameter2 / diameter1;
        let sinb = (2.0 * ((c * s).powi(2) + c.powi(2) + s.powi(2)) - c.powi(4) - s.powi(4) - 1.0)
            .sqrt()
            / (2.0 * c);
        let sinb = sinb.min(1.0);
        let sina = (sinb / s).min(1.0);
        let b = sinb.asin();
        let a = sina.asin();
        let area = (s.powi(2) * a + b) - (s.powi(2) * sina * a.cos() + sinb * b.cos());
        let fraction1 = area / std::f64::consts::PI;
        let fraction2 = fraction1 / s.powi(2);
        (fraction1, fraction2)
    };

    // w > 0: antenna 1 is in front and antenna 2 behind.
    let antenna1_in_front = uvw_m[2] > 0.0;
    let antenna2_in_front = uvw_m[2] < 0.0;
    let (clear_antenna2, clear_antenna1) = match charged {
        ShadowedAntenna::Behind => (antenna2_in_front, antenna1_in_front),
        ShadowedAntenna::CasaSimulatorFront => (antenna1_in_front, antenna2_in_front),
    };
    if clear_antenna2 {
        fraction2 = 0.0;
    }
    if clear_antenna1 {
        fraction1 = 0.0;
    }
    (fraction1, fraction2)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn baseline(antenna1: usize, antenna2: usize, uvw_m: [f64; 3]) -> ProjectedBaseline {
        ProjectedBaseline {
            antenna1,
            antenna2,
            uvw_m,
            diameter1_m: 25.0,
            diameter2_m: 25.0,
        }
    }

    #[test]
    fn casa_simulator_charges_the_antenna_in_front() {
        let shadowed = shadowed_antennas(
            3,
            [
                baseline(0, 1, [10.0, 0.0, 1.0]),
                baseline(0, 2, [100.0, 0.0, -1.0]),
                baseline(1, 2, [10.0, 0.0, -1.0]),
            ],
            CASA_SHADOW_FRACTION_LIMIT,
            ShadowedAntenna::CasaSimulatorFront,
        );
        assert_eq!(shadowed, vec![true, false, true]);
    }

    #[test]
    fn the_antenna_behind_is_shadowed() {
        // uvw = R·(X1 − X2): w > 0 puts antenna 1 nearer the source, so the
        // dish behind it, antenna 2, is the shadowed one.
        let shadowed = shadowed_antennas(
            3,
            [
                baseline(0, 1, [10.0, 0.0, 1.0]),
                baseline(0, 2, [100.0, 0.0, -1.0]),
                baseline(1, 2, [10.0, 0.0, -1.0]),
            ],
            CASA_SHADOW_FRACTION_LIMIT,
            ShadowedAntenna::Behind,
        );
        // Baseline (1, 2) has w < 0: antenna 2 is in front, antenna 1 behind.
        assert_eq!(shadowed, vec![false, true, false]);
    }

    #[test]
    fn a_blockage_at_the_limit_flags_nothing() {
        let (fraction1, fraction2) = blockage_fractions(
            [24.999, 0.0, 1.0],
            25.0,
            25.0,
            ShadowedAntenna::CasaSimulatorFront,
        );
        assert!(fraction1 > 0.0);
        assert!(fraction1 < CASA_SHADOW_FRACTION_LIMIT);
        assert_eq!(fraction2, 0.0);
        let shadowed = shadowed_antennas(
            2,
            [baseline(0, 1, [24.999, 0.0, 1.0])],
            CASA_SHADOW_FRACTION_LIMIT,
            ShadowedAntenna::CasaSimulatorFront,
        );
        assert_eq!(shadowed, vec![false, false]);
    }

    #[test]
    fn a_larger_limit_tolerates_a_partial_blockage() {
        let partial = [baseline(0, 1, [20.0, 0.0, 1.0])];
        let strict = shadowed_antennas(2, partial, 1e-6, ShadowedAntenna::Behind);
        let loose = shadowed_antennas(2, partial, 0.5, ShadowedAntenna::Behind);
        assert_eq!(strict, vec![false, true]);
        assert_eq!(loose, vec![false, false]);
    }
}
