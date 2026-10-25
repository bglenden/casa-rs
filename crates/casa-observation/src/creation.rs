// SPDX-License-Identifier: LGPL-3.0-or-later
//! What a producer declares before writing a dataset: the sample grid of
//! one partition. The rest of the scientific description is the common
//! [`Catalogue`](crate::Catalogue); chunks, codecs and paths are backend
//! storage options. One declaration serves an MSv4 creator and an MSv2
//! creator alike: cells in time order become MSv2 rows, and padded cells
//! become flagged NaN rows.

use casa_types::measures::{EpochRef, MjdHighPrec};

use crate::ObservationError;
use crate::buffers::BatchBuffers;
use crate::catalogue::{FieldId, OrderedBaseline, ScanId};
use crate::cursor::VisibilityCursor;
use crate::grouping::ReadEvent;

/// The time × baseline rectangle of one partition, fixed before any payload
/// is written: its baselines, its times with the field and scan of each,
/// and the integration interval.
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
    /// Integration interval in seconds, one for the dataset.
    pub interval_s: f64,
}

impl TimeBaselineGrid {
    /// The grid of everything one partition's cursor delivers: its times in
    /// order with the field and scan at each, its baseline table, and its
    /// one interval. The cursor must read the interval and scan roles.
    ///
    /// # Errors
    ///
    /// When the traversal fails, lacks the interval or scan, delivers no
    /// time, or has more than one interval: a grid cannot represent that,
    /// and the producer must say how to divide the samples first.
    pub fn from_cursor(cursor: &mut dyn VisibilityCursor) -> Result<Self, ObservationError> {
        let mut buffers = BatchBuffers::default();
        let mut grid: Option<Self> = None;
        while let Some(event) = cursor.next(&mut buffers)? {
            let ReadEvent::Batch(batch) = event else {
                continue;
            };
            let (Some(scan), Some(interval)) = (batch.scan, batch.timing.interval_s) else {
                return Err(ObservationError::InvalidRequest {
                    reason: "a grid needs the traversal's interval and scan roles".to_string(),
                });
            };
            let grid = grid.get_or_insert_with(|| Self {
                baselines: batch.baselines.to_vec(),
                time_scale: batch.time.reference(),
                time_origin: batch.time.origin(),
                times_s: Vec::new(),
                field_per_time: Vec::new(),
                scan_per_time: Vec::new(),
                interval_s: interval.get(0, batch.samples_per_time()),
            });
            for sample in 0..batch.samples() {
                if batch.interval_s(sample) != Some(grid.interval_s) {
                    return Err(ObservationError::Unsupported {
                        what: "a grid with more than one integration interval",
                    });
                }
            }
            for (t, &offset) in batch.time.offsets_s().iter().enumerate() {
                if grid.times_s.last().is_some_and(|last| *last >= offset) {
                    return Err(ObservationError::InvalidStore {
                        reason: format!("time {offset} does not follow the previous time"),
                    });
                }
                grid.times_s.push(offset);
                grid.field_per_time.push(batch.field[t]);
                grid.scan_per_time.push(scan[t]);
            }
        }
        grid.ok_or_else(|| ObservationError::InvalidRequest {
            reason: "the traversal delivered no samples".to_string(),
        })
    }
}
