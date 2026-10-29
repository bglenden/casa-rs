// SPDX-License-Identifier: LGPL-3.0-or-later
//! Scan numbers of a MeasurementSet created from several observations, as
//! CASA's `concat` offsets them (`MSConcat::concatenate`,
//! `MSConcat.cc:1419-1590`), so that the scans of an appended input do not
//! repeat those already written.
//!
//! Inputs are appended in order. For each, the destination's observing
//! sessions are taken in the order they first appear in its MAIN, each with
//! the smallest and largest scan number it holds (the numbers written,
//! after earlier offsets). Each session's offset is:
//! - for the first, its smallest scan less 1;
//! - for one an incoming session matched (its counterpart), the previous
//!   session's largest scan plus 1, less the incoming input's smallest scan
//!   of all;
//! - for any other, its smallest scan less 1;
//!
//! and a session new to the destination takes the default, the
//! destination's largest scan (at least 0) plus 1 less the incoming
//! smallest. Every offset is floored at 0. An incoming row's scan is offset
//! only when its observation ID changed, comparing the number it is
//! written with against its own, as CASA compares them.

use std::collections::BTreeMap;

/// One observing session of the destination as the offsets see it: its
/// observation ID and the smallest and largest scan number it holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Session {
    pub(super) id: i32,
    pub(super) scans: [i32; 2],
}

/// One incoming observing session: its own observation ID, the ID it is
/// written with, and whether it matched a destination session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Incoming {
    pub(super) own: i32,
    pub(super) written: i32,
    pub(super) counterpart: bool,
}

/// The offset added to the scans of each of `incoming`, appended to a
/// destination whose sessions are `destination` in the order they first
/// appear, `smallest` being the smallest scan of the whole incoming input.
pub(super) fn offsets(destination: &[Session], incoming: &[Incoming], smallest: i32) -> Vec<i32> {
    let largest = destination
        .iter()
        .map(|session| session.scans[1])
        .fold(0, i32::max);
    let default = (largest + 1 - smallest).max(0);
    let matched: Vec<i32> = incoming
        .iter()
        .filter(|session| session.counterpart)
        .map(|session| session.written)
        .collect();
    let mut by_id = BTreeMap::new();
    for (index, session) in destination.iter().enumerate() {
        let offset = if index != 0 && matched.contains(&session.id) {
            destination[index - 1].scans[1] + 1 - smallest
        } else {
            session.scans[0] - 1
        };
        by_id.insert(session.id, offset.max(0));
    }
    incoming
        .iter()
        .map(|session| {
            if session.written == session.own {
                0
            } else {
                by_id.get(&session.written).copied().unwrap_or(default)
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{Incoming, Session, offsets};

    fn session(id: i32, scans: [i32; 2]) -> Session {
        Session { id, scans }
    }

    fn incoming(own: i32, written: i32, counterpart: bool) -> Incoming {
        Incoming {
            own,
            written,
            counterpart,
        }
    }

    /// A new session is offset past every scan already written.
    #[test]
    fn a_new_session_follows_every_scan_written() {
        let destination = [session(0, [1, 20])];
        assert_eq!(offsets(&destination, &[incoming(0, 1, false)], 1), [20]);
        assert_eq!(
            offsets(&destination, &[incoming(0, 1, false)], 100),
            [0],
            "floored at 0"
        );
    }

    /// The destination's MAIN visits observation 1 (scans up to 10) before
    /// observation 0 (up to 20): an incoming session matching observation 0
    /// continues after observation 1's largest scan, the previous in order
    /// of appearance, not after observation 0's own.
    #[test]
    fn a_counterpart_continues_from_the_previous_session_in_order_of_appearance() {
        let destination = [session(1, [1, 10]), session(0, [11, 20])];
        assert_eq!(offsets(&destination, &[incoming(1, 0, true)], 1), [10]);
    }

    /// Incoming sessions whose smallest scans are 1 and 100 are offset by
    /// the input's smallest of all.
    #[test]
    fn offsets_count_from_the_inputs_smallest_scan() {
        let destination = [session(0, [1, 20])];
        let incoming = [incoming(0, 1, false), incoming(1, 2, false)];
        assert_eq!(offsets(&destination, &incoming, 1), [20, 20]);
    }

    /// A session keeping its observation ID keeps its scans, whatever it
    /// matched: the split-and-concatenated case.
    #[test]
    fn a_session_keeping_its_id_keeps_its_scans() {
        let destination = [session(0, [1, 20])];
        assert_eq!(offsets(&destination, &[incoming(0, 0, true)], 1), [0]);
        assert_eq!(offsets(&destination, &[incoming(0, 0, false)], 5), [0]);
    }

    /// The first session in order of appearance is offset by its own
    /// smallest scan less 1 even when matched; a later counterpart uses the
    /// previous session's largest.
    #[test]
    fn the_first_session_takes_its_smallest_scan() {
        let destination = [session(0, [3, 8]), session(1, [9, 15])];
        let incoming = [incoming(1, 0, true), incoming(0, 1, true)];
        assert_eq!(offsets(&destination, &incoming, 1), [2, 8]);
    }
}
