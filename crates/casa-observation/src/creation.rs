// SPDX-License-Identifier: LGPL-3.0-or-later
//! What a producer declares before writing a factored (time × baseline)
//! dataset: the sample grid. The rest of the scientific description is the
//! common [`Catalogue`](crate::Catalogue); chunks, codecs and paths are
//! backend storage options.

use std::collections::{BTreeMap, BTreeSet};

use casa_types::measures::{EpochRef, MjdHighPrec};

use crate::ObservationError;
use crate::buffers::BatchBuffers;
use crate::catalogue::{FieldId, OrderedBaseline, ScanId};
use crate::cursor::VisibilityCursor;
use crate::grouping::ReadEvent;

/// The time × baseline rectangle of a factored dataset, fixed before any
/// payload is written: its baselines, its times with the field and scan of
/// each, and the integration interval.
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
    /// The grid of everything a traversal delivers: its distinct times in
    /// order, the field and scan at each, its baselines in antenna order,
    /// and its one interval. The traversal must read the interval and scan
    /// roles.
    ///
    /// # Errors
    ///
    /// When the traversal fails, lacks the interval or scan, or has two
    /// fields or scans at one time or more than one interval: a grid cannot
    /// represent those, and the producer must say how to divide the samples
    /// first.
    pub fn from_traversal(cursor: &mut dyn VisibilityCursor) -> Result<Self, ObservationError> {
        let mut buffers = BatchBuffers::default();
        let mut times = BTreeMap::<u64, (f64, FieldId, ScanId)>::new();
        let mut baselines = BTreeSet::new();
        let mut frame: Option<(EpochRef, MjdHighPrec)> = None;
        let mut interval: Option<f64> = None;
        while let Some(event) = cursor.next(&mut buffers)? {
            let ReadEvent::Batch(batch) = event else {
                continue;
            };
            let time = batch.topology.time();
            let (scale, origin) = *frame.get_or_insert((time.reference(), time.origin()));
            if time.reference() != scale {
                return Err(ObservationError::Unsupported {
                    what: "a grid of times in more than one scale",
                });
            }
            let per_time = batch.topology.samples_per_time();
            for sample in 0..batch.samples() {
                let coordinate = batch.topology.sample(sample);
                baselines.insert(coordinate.baseline);
                let (Some(scan), Some(sample_interval)) =
                    (batch.scan(sample), batch.interval_s(sample))
                else {
                    return Err(ObservationError::InvalidRequest {
                        reason: "a grid needs the traversal's interval and scan roles".to_string(),
                    });
                };
                if *interval.get_or_insert(sample_interval) != sample_interval {
                    return Err(ObservationError::Unsupported {
                        what: "a grid with more than one integration interval",
                    });
                }
                let seconds = time.seconds_since(sample / per_time, origin);
                let entry =
                    times
                        .entry(seconds.to_bits())
                        .or_insert((seconds, coordinate.field, scan));
                if (entry.1, entry.2) != (coordinate.field, scan) {
                    return Err(ObservationError::Unsupported {
                        what: "a grid time with two fields or scans",
                    });
                }
            }
        }
        let (time_scale, time_origin) = frame.ok_or(ObservationError::InvalidRequest {
            reason: "the traversal delivered no samples".to_string(),
        })?;
        let mut entries: Vec<_> = times.into_values().collect();
        entries.sort_by(|a, b| a.0.total_cmp(&b.0));
        Ok(Self {
            baselines: baselines.into_iter().collect(),
            time_scale,
            time_origin,
            times_s: entries.iter().map(|entry| entry.0).collect(),
            field_per_time: entries.iter().map(|entry| entry.1).collect(),
            scan_per_time: entries.iter().map(|entry| entry.2).collect(),
            interval_s: interval.unwrap_or(0.0),
        })
    }
}
