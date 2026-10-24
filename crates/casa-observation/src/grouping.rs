// SPDX-License-Identifier: LGPL-3.0-or-later
//! Scientific groups that span batches, and the events a cursor delivers.

use std::ops::Range;

use crate::batch::VisibilityBatch;
use crate::catalogue::{FieldId, PartitionId};

/// A scientific group: the samples a consumer treats together, such as one
/// gain solution interval. Batches and storage chunks do not bound it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupKey {
    /// The partition (calibration does not combine spectral windows in the
    /// trial).
    pub partition: PartitionId,
    /// The field, unless the request combines fields.
    pub field: Option<FieldId>,
    /// The time bin, counted from the request's bin origin.
    pub bin: u64,
}

/// Where a fragment lies in its group.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FragmentPosition {
    /// The whole group: no earlier or later fragments.
    Whole,
    /// The group's first fragment; more follow.
    First,
    /// Neither first nor last.
    Middle,
    /// The group's last fragment.
    Last,
}

impl FragmentPosition {
    /// Whether this fragment completes its group.
    #[must_use]
    pub fn closes(self) -> bool {
        matches!(self, Self::Whole | Self::Last)
    }
}

/// The samples of one group within one batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupFragment {
    /// The group.
    pub key: GroupKey,
    /// The batch's samples that belong to it: a contiguous range in the
    /// trial, which delivers time-ordered traversals only.
    pub samples: Range<usize>,
    /// Where the fragment lies in its group. `Last` or `Whole` is given only
    /// when the source has proven no later sample belongs to the group;
    /// otherwise a later [`ReadEvent::GroupEnd`] closes it.
    pub position: FragmentPosition,
}

/// What a cursor delivers.
#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "a batch is a few hundred bytes of views, returned once per batch; boxing it would allocate per batch"
)]
pub enum ReadEvent<'a> {
    /// The next batch.
    Batch(VisibilityBatch<'a>),
    /// No further samples of this group will arrive. The group's earlier
    /// fragments may still be in a consumer's work units: input is complete,
    /// the work need not be.
    GroupEnd(GroupKey),
}
