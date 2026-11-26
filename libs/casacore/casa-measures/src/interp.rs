// SPDX-License-Identifier: LGPL-3.0-or-later
//! Linear interpolation for EOP values.
//!
//! Provides interpolation of Earth Orientation Parameters between daily
//! entries. For dUT1, the interpolation avoids interpolating across
//! leap-second boundaries (where dUT1 can jump by ~1 second).

use super::{EopEntry, EopValues};

/// Linearly interpolate EOP values at a given MJD from a sorted entry slice.
///
/// Returns `None` if `mjd` is outside the table range.
///
/// For dUT1, if the two bracketing entries span a leap-second jump
/// (|dUT1 difference| > 0.5 s), the nearest entry's value is used
/// instead of interpolating, to avoid producing a nonsensical midpoint.
pub(crate) fn interpolate(entries: &[EopEntry], mjd: f64) -> Option<EopValues> {
    if entries.is_empty() {
        return None;
    }

    let first_mjd = entries[0].mjd;
    let last_mjd = entries[entries.len() - 1].mjd;

    if mjd < first_mjd || mjd > last_mjd {
        return None;
    }

    // Fast index calculation: entries are daily with ~1 day spacing
    let idx_f = mjd - first_mjd;
    let idx = idx_f as usize;

    if idx >= entries.len() - 1 {
        // At or past last entry — return the last entry
        let e = &entries[entries.len() - 1];
        return Some(EopValues {
            dut1_seconds: e.dut1_seconds,
            x_arcsec: e.x_arcsec,
            y_arcsec: e.y_arcsec,
            dx_mas: e.dx_mas,
            dy_mas: e.dy_mas,
            is_predicted: e.is_predicted,
        });
    }

    let e0 = &entries[idx];
    let e1 = &entries[idx + 1];

    // Fractional position between the two entries
    let t = (mjd - e0.mjd) / (e1.mjd - e0.mjd);

    // dUT1: check for leap-second jump
    let dut1 = if (e1.dut1_seconds - e0.dut1_seconds).abs() > 0.5 {
        // Leap-second boundary — use nearest value
        if t < 0.5 {
            e0.dut1_seconds
        } else {
            e1.dut1_seconds
        }
    } else {
        lerp(e0.dut1_seconds, e1.dut1_seconds, t)
    };

    Some(EopValues {
        dut1_seconds: dut1,
        x_arcsec: lerp(e0.x_arcsec, e1.x_arcsec, t),
        y_arcsec: lerp(e0.y_arcsec, e1.y_arcsec, t),
        dx_mas: lerp(e0.dx_mas, e1.dx_mas, t),
        dy_mas: lerp(e0.dy_mas, e1.dy_mas, t),
        is_predicted: e0.is_predicted || e1.is_predicted,
    })
}

#[inline]
fn lerp(a: f64, b: f64, t: f64) -> f64 {
    a + t * (b - a)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entries(dut1s: &[(f64, f64)]) -> Vec<EopEntry> {
        dut1s
            .iter()
            .map(|&(mjd, dut1)| EopEntry {
                mjd,
                x_arcsec: 0.1,
                y_arcsec: 0.2,
                dut1_seconds: dut1,
                lod_seconds: 0.0,
                dx_mas: 0.0,
                dy_mas: 0.0,
                is_predicted: false,
            })
            .collect()
    }

    #[test]
    fn interpolate_midpoint() {
        let entries = make_entries(&[(51544.0, 0.3), (51545.0, 0.5)]);
        let v = interpolate(&entries, 51544.5).unwrap();
        assert!((v.dut1_seconds - 0.4).abs() < 1e-10);
    }

    #[test]
    fn interpolate_at_entry() {
        let entries = make_entries(&[(51544.0, 0.3), (51545.0, 0.5)]);
        let v = interpolate(&entries, 51544.0).unwrap();
        assert!((v.dut1_seconds - 0.3).abs() < 1e-10);
    }

    #[test]
    fn interpolate_outside_range() {
        let entries = make_entries(&[(51544.0, 0.3), (51545.0, 0.5)]);
        assert!(interpolate(&entries, 51543.0).is_none());
        assert!(interpolate(&entries, 51546.0).is_none());
    }

    #[test]
    fn interpolate_leap_second_jump() {
        // Simulate a leap second: dUT1 jumps by ~1s
        let entries = make_entries(&[(51544.0, 0.8), (51545.0, -0.2)]);
        let v = interpolate(&entries, 51544.3).unwrap();
        // Should use nearest (first entry) since |jump| > 0.5
        assert!((v.dut1_seconds - 0.8).abs() < 1e-10);

        let v2 = interpolate(&entries, 51544.7).unwrap();
        assert!((v2.dut1_seconds - (-0.2)).abs() < 1e-10);
    }

    #[test]
    fn interpolate_polar_motion() {
        let mut entries = make_entries(&[(51544.0, 0.3), (51545.0, 0.5)]);
        entries[0].x_arcsec = 0.1;
        entries[1].x_arcsec = 0.3;
        let v = interpolate(&entries, 51544.5).unwrap();
        assert!((v.x_arcsec - 0.2).abs() < 1e-10);
    }
}
