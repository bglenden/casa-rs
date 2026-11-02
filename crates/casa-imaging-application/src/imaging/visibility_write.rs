// SPDX-License-Identifier: LGPL-3.0-or-later
//! Visibilities written back in the final pass: the final model into
//! `MODEL_DATA` and the continuum-subtracted data into `CORRECTED_DATA`,
//! through the batch route's update session.

use casa_imaging_model::{ContinuumFitRule, SequentialContinuumTransform};
use casa_imaging_runtime::pass::SourceError;

use super::observation_source::BatchRoute;
pub(crate) use super::observation_source::BatchWriter as VisibilityWriter;

/// Where the visibilities go: the update session of the batch route the
/// run reads.
#[derive(Clone, Copy)]
pub(crate) struct VisibilityWriteTarget<'a>(pub(crate) &'a BatchRoute);

impl<'a> VisibilityWriteTarget<'a> {
    /// Heap bytes the write holds at most, admitted with the pass that
    /// writes ([`BatchRoute::write_bytes`]).
    pub(crate) fn retained_bytes(&self) -> Result<u64, SourceError> {
        self.0.write_bytes()
    }

    /// Begin the write; `transform` is the continuum transform whose output
    /// `CORRECTED_DATA` holds.
    pub(crate) fn begin(
        &self,
        transform: Option<&'a SequentialContinuumTransform>,
    ) -> Result<VisibilityWriter<'a>, SourceError> {
        VisibilityWriter::begin(
            self.0
                .write_back()
                .ok_or("the batch route was opened to read only")?,
            transform,
        )
    }
}

/// Whether a channel of a row under the continuum `rule` reaches the line
/// output, and so is written to either column: every channel without a
/// continuum transform; fit-only channels keep their contents.
pub(super) fn writes_channel(rule: Option<&ContinuumFitRule>, channel: u32) -> bool {
    rule.is_none_or(|rule| {
        rule.channel_use(channel)
            .is_some_and(|role| role.contributes_to_output())
    })
}
