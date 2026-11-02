// SPDX-License-Identifier: LGPL-3.0-or-later
//! What a producer declares before writing a dataset: the sample grid of
//! one partition. The rest of the scientific description is the common
//! [`Catalogue`](crate::Catalogue); chunks, codecs and paths are backend
//! storage options. One declaration serves an MSv4 creator and an MSv2
//! creator alike: cells in time order become MSv2 rows, and padded cells
//! become flagged NaN rows.

use casa_types::measures::{EpochRef, MjdHighPrec};

use crate::catalogue::{FieldId, OrderedBaseline, ScanId};

/// The time × baseline rectangle of one partition, fixed before any payload
/// is written: its baselines, its times with the field and scan of each,
/// and the integration interval.
///
/// [`Observation::grid`](crate::Observation::grid) gives an opened
/// partition's from the store's index, reading no payload. A cell no
/// acquisition fills is still on the grid: it is written as padding, and
/// its interval is not compared with the dataset's.
#[derive(Clone, Debug, PartialEq)]
pub struct TimeBaselineGrid {
    /// Baselines, in dataset order.
    pub baselines: Vec<OrderedBaseline>,
    /// Time scale of the times.
    pub time_scale: EpochRef,
    /// Origin of the times.
    pub time_origin: MjdHighPrec,
    /// Times in seconds from `time_origin`, strictly ascending.
    pub times_s: Vec<f64>,
    /// Field at each time.
    pub field_per_time: Vec<FieldId>,
    /// Scan at each time.
    pub scan_per_time: Vec<ScanId>,
    /// Integration interval in seconds, one for the dataset: every
    /// acquisition's.
    pub interval_s: f64,
}
