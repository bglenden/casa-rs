// SPDX-License-Identifier: LGPL-3.0-or-later
//! Feeds: `sm.setfeed`, and the feeds `observe` creates when none are set.
//!
//! `NewMSSimulator::initFeeds` appends one FEED row per antenna (and per
//! feed in list mode) for the antennas defined when it is called.

use super::error::{SimulatorError, SimulatorResult};

/// The feed modes `Simulator::setfeed` accepts.
const MODES: [&str; 3] = ["perfect R L", "perfect X Y", "list"];

/// One FEED row.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Feed {
    pub(crate) antenna_id: i32,
    pub(crate) feed_id: i32,
    /// POLARIZATION_TYPE of the two receptors.
    pub(crate) polarization: [&'static str; 2],
}

/// The FEED rows of `setfeed(mode, x, y, pol)` for `antenna_count`
/// antennas.
///
/// Only one feed per antenna is supported: CASA chooses list mode, one feed
/// per position, whenever `x` has more than one element, and the simulator
/// writes such multi-feed observations from Phase 2 on.
pub(crate) fn feeds(
    mode: &str,
    x: &[f64],
    y: &[f64],
    pol: &[String],
    antenna_count: usize,
) -> SimulatorResult<Vec<Feed>> {
    if !MODES.contains(&mode) {
        return Err(SimulatorError::refused(
            "Currently, only perfect R L or perfect X Y feeds or list are recognized",
        ));
    }
    if antenna_count == 0 {
        // CASA logs this, then aborts building an empty table slice.
        return Err(SimulatorError::raised(
            "NewMSSimulator::initFeeds: must call initAnt() first",
        ));
    }
    if x.len() > 1 {
        if y.len() != x.len() {
            return Err(SimulatorError::raised(
                "Feed x and y must be the same length",
            ));
        }
        if pol.len() != x.len() {
            return Err(SimulatorError::raised(
                "Feed polarization list must be same length as the number of positions",
            ));
        }
        return Err(SimulatorError::raised(
            "setfeed: more than one feed per antenna (list mode) is not yet implemented in casa-rs",
        ));
    }
    Ok(perfect_feeds(mode, antenna_count))
}

/// One perfect feed per antenna: X and Y receptors if `mode` contains an
/// uppercase X, otherwise R and L.
pub(crate) fn perfect_feeds(mode: &str, antenna_count: usize) -> Vec<Feed> {
    let polarization = if mode.contains('X') {
        ["X", "Y"]
    } else {
        ["R", "L"]
    };
    (0..antenna_count)
        .map(|antenna| Feed {
            antenna_id: antenna as i32,
            feed_id: 0,
            polarization,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn perfect_modes_choose_the_receptors() {
        let linear = feeds("perfect X Y", &[0.0], &[0.0], &["R".to_string()], 2).unwrap();
        assert_eq!(linear.len(), 2);
        assert_eq!(linear[1].antenna_id, 1);
        assert_eq!(linear[0].polarization, ["X", "Y"]);
        let circular = feeds("perfect R L", &[0.0], &[0.0], &["R".to_string()], 1).unwrap();
        assert_eq!(circular[0].polarization, ["R", "L"]);
        // A one-position list is a perfect feed.
        let list = feeds("list", &[0.0], &[0.0], &["X".to_string()], 1).unwrap();
        assert_eq!(list[0].polarization, ["R", "L"]);
    }

    #[test]
    fn unknown_modes_and_missing_antennas_are_refused() {
        let error = feeds("", &[0.0], &[0.0], &[], 3).unwrap_err();
        assert_eq!(
            error.outcome(),
            super::super::error::CasaOutcome::ReturnsFalse
        );
        assert!(feeds("perfect R L", &[0.0], &[0.0], &[], 0).is_err());
    }
}
