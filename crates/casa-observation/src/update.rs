// SPDX-License-Identifier: LGPL-3.0-or-later
//! Writing back: what an update session may write, and the values one
//! delivered batch's samples receive (`docs/measurement-set/contracts.md`,
//! section 7, "Updates").
//!
//! An update addresses the samples of a batch the same session delivered:
//! the store maps them to its own native units (MAIN rows, Zarr chunks),
//! and a sample with no native address (MSv2 padding) receives nothing. The
//! targets are declared when the session opens, so the structural preflight
//! (the variable exists, the data groups sharing it are accepted) runs
//! before any payload. Values are checked against the batch before anything
//! is written; a refused write writes nothing of its batch.
//!
//! **Trial status.** This is the write-back trial (`docs/measurement-set/
//! trial/README.md`, "Write-back"): one updater per traversal, written batch
//! for batch in delivery order, boolean flags only; the slices of a
//! [`TimeSplit`](crate::TimeSplit) each have their own updater, live at
//! once.

use ndarray::{ArrayView3, Axis};
use num_complex::Complex32;

use crate::{ObservationError, VisibilityBatch, WeightRule};

/// A role a session writes, of one data group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WritableRole {
    /// The group's visibilities (MSv2 `DATA`, `CORRECTED_DATA` or
    /// `MODEL_DATA`; MSv4 the group's correlated-data variable).
    Visibilities,
    /// The group's element flags (`FLAG`). Row flags (`FLAG_ROW`) and flag
    /// categories are never written: a flag change cannot clear them.
    Flags,
    /// The group's weights (`WEIGHT`, `WEIGHT_SPECTRUM` or MSv4 `WEIGHT`).
    Weights,
}

/// Whether a target may write a variable other data groups share: data
/// groups commonly share their flags and weights, so writing one group's
/// flags changes every group's.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum AliasScope {
    /// Refuse the target when another group shares its variable.
    #[default]
    Refuse,
    /// Write the shared variable, for every group that shares it.
    Accept,
}

/// One thing an update session may write, declared when it opens.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateTarget {
    /// The data group, by name.
    pub data_group: String,
    /// The role.
    pub role: WritableRole,
    /// Whether the variable may be shared with other groups.
    pub aliases: AliasScope,
    /// Create the group's visibility variable when the store lacks it,
    /// initialized as CASA initializes it: `corrected` from the `base`
    /// group's visibilities (`CORRECTED_DATA` from `DATA`), so what a pass
    /// skips reads as uncorrected; `model` with a unit point source at the
    /// phase centre (1, and 0 in the cross hands), so what a pass skips
    /// reads as CASA's default model. Stores that cannot add a variable
    /// refuse.
    pub create: bool,
}

impl UpdateTarget {
    /// A target that writes `role` of `data_group`, refusing aliases and
    /// creating nothing.
    #[must_use]
    pub fn new(data_group: impl Into<String>, role: WritableRole) -> Self {
        Self {
            data_group: data_group.into(),
            role,
            aliases: AliasScope::Refuse,
            create: false,
        }
    }
}

/// A target as the store resolved it: the native variable it writes and
/// the other groups that share that variable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedTarget {
    /// The declared target.
    pub target: UpdateTarget,
    /// The native variable or column written.
    pub variable: String,
    /// The other data groups that read the same variable, which the write
    /// changes too.
    pub aliases: Vec<String>,
    /// Whether the session creates the variable.
    pub created: bool,
}

/// What a store's updates guarantee on failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UpdateGuarantees {
    /// Each physical unit (an MSv4 chunk object) is replaced whole: a
    /// failure or a killed process leaves the old unit or the new one,
    /// never a mixture. Units already replaced stay replaced: there is no
    /// transaction across units.
    pub atomic_units: bool,
    /// Another process is excluded while an updater writes (casacore's
    /// table lock for MSv2). MSv4 has no cross-process lock protocol.
    pub cross_process_lock: bool,
}

/// How a weight patch treats channels the native weight shares: an MSv2
/// `WEIGHT` is one value per row and correlation that every channel reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WeightChannels {
    /// Change the delivered channels only. Where the native weight is
    /// shared by channels, this is refused unless the batch delivers every
    /// channel and gives each sample and product one value: the patch must
    /// not change a channel it does not address.
    Delivered,
    /// Change every channel of the partition: the patch gives each sample
    /// and product one value, which every channel takes.
    All,
}

/// The new values of one target for the samples, channels and products of
/// the batch they are written against, each `[sample, channel, product]`.
#[derive(Clone, Copy, Debug)]
pub enum PatchValues<'a> {
    /// Replace the visibilities.
    Visibilities(ArrayView3<'a, Complex32>),
    /// Change element flags: `set` sets and `clear` clears where true;
    /// elements neither names keep their stored flag. A sample may not be
    /// both set and cleared.
    Flags {
        /// Flags to set.
        set: Option<ArrayView3<'a, bool>>,
        /// Flags to clear.
        clear: Option<ArrayView3<'a, bool>>,
    },
    /// Replace the weights.
    Weights {
        /// The weights.
        values: ArrayView3<'a, f32>,
        /// Which channels they change.
        channels: WeightChannels,
    },
}

impl PatchValues<'_> {
    /// The role these values write.
    #[must_use]
    pub fn role(&self) -> WritableRole {
        match self {
            Self::Visibilities(_) => WritableRole::Visibilities,
            Self::Flags { .. } => WritableRole::Flags,
            Self::Weights { .. } => WritableRole::Weights,
        }
    }
}

/// What an updater wrote, for the evidence.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UpdateReport {
    /// Batches written.
    pub batches: usize,
    /// Samples given a native address and written.
    pub samples: usize,
    /// Samples with no native address (MSv2 padding): nothing was written
    /// for them.
    pub unaddressed: usize,
    /// Native units written: MSv2 cells, MSv4 chunk objects.
    pub units_written: usize,
    /// Native units read back to merge a change into what they hold: an
    /// MSv2 cell a patch covers only partly, an MSv4 chunk a flush covers
    /// only partly. A unit a patch supplies whole is never read.
    pub units_read: usize,
    /// The most bytes the updater holds between batches: MSv4, one
    /// chunk-row of times of each target variable over the channel chunks
    /// the traversal reaches (every channel for weights); MSv2, none.
    pub held_bytes: usize,
}

/// Writes one traversal's batches back to its store.
///
/// An updater is bound to a traversal and to some of its session's
/// targets (`casa_ms::observation_io::UpdateSession::updater`); each call
/// gives the values of those targets, in that order, for one delivered
/// batch.
///
/// Only [`Self::finish`] completes an update: an updater dropped without
/// it leaves what it wrote and discards what it held (an MSv4 updater's
/// window of up to one chunk-row of times), and reports neither.
pub trait Updater: Send {
    /// Write `values`, one per bound target, for `batch`'s samples.
    ///
    /// # Errors
    ///
    /// When a value disagrees with its target or the batch, the batch is
    /// not the traversal's next, or a change is refused (a weight shared by
    /// channels the patch does not address, a flag cleared or a finite
    /// weight written where the store holds padding). Nothing of `batch` is
    /// then written. A storage failure
    /// leaves what the store's guarantees state.
    fn write(
        &mut self,
        batch: &VisibilityBatch<'_>,
        values: &[PatchValues<'_>],
    ) -> Result<(), ObservationError>;

    /// Finish: write what is still held and report.
    ///
    /// # Errors
    ///
    /// When a batch of the traversal was never written (a store that plans
    /// its writes up front needs every one), or storage fails.
    fn finish(self: Box<Self>) -> Result<UpdateReport, ObservationError>;
}

/// Check `values` against the bound `roles` and `batch` before anything is
/// written: one value per role, in order, each of the batch's shape; no
/// sample both set and cleared; and a weight patch for every channel giving
/// each sample and product one value.
///
/// # Errors
///
/// [`ObservationError::InvalidRequest`] naming the first disagreement.
pub fn check_values(
    batch: &VisibilityBatch<'_>,
    roles: &[WritableRole],
    values: &[PatchValues<'_>],
) -> Result<(), ObservationError> {
    let refuse = |reason: String| Err(ObservationError::InvalidRequest { reason });
    if values.len() != roles.len() {
        return refuse(format!(
            "{} patch values for {} bound targets",
            values.len(),
            roles.len()
        ));
    }
    let shape = batch.shape();
    for (index, (value, role)) in values.iter().zip(roles).enumerate() {
        if value.role() != *role {
            return refuse(format!(
                "patch value {index} writes {:?}; its target is {role:?}",
                value.role()
            ));
        }
        let shapes: Vec<&[usize]> = match value {
            PatchValues::Visibilities(values) => vec![values.shape()],
            PatchValues::Flags { set, clear } => {
                set.iter().chain(clear).map(|mask| mask.shape()).collect()
            }
            PatchValues::Weights { values, .. } => vec![values.shape()],
        };
        if let Some(wrong) = shapes.iter().find(|actual| **actual != shape.as_slice()) {
            return refuse(format!(
                "patch value {index} has shape {wrong:?}; the batch's is {shape:?}"
            ));
        }
        match value {
            PatchValues::Flags {
                set: Some(set),
                clear: Some(clear),
            } if set.iter().zip(clear).any(|(set, clear)| *set && *clear) => {
                return refuse(format!("patch value {index} both sets and clears a flag"));
            }
            // Weights patched from weights made from the sigmas would write
            // sigmas back as weights, unannounced.
            PatchValues::Weights { .. } if batch.group.weight_rule != WeightRule::Stored => {
                return refuse(format!(
                    "patch value {index} writes weights for a batch whose weights were made \
                     from the sigmas ({:?}); patch weights from a traversal of the stored \
                     weights",
                    batch.group.weight_rule
                ));
            }
            PatchValues::Weights {
                values,
                channels: WeightChannels::All,
            } if !uniform_over_channels(values) => {
                return refuse(format!(
                    "patch value {index} changes every channel's weight but gives a sample's \
                     channels different values"
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Whether every sample and product has one value across channels.
#[must_use]
pub fn uniform_over_channels(values: &ArrayView3<'_, f32>) -> bool {
    values.axis_iter(Axis(0)).all(|sample| {
        let first = sample.index_axis(Axis(0), 0);
        sample.axis_iter(Axis(0)).all(|channel| {
            channel
                .iter()
                .zip(&first)
                .all(|(a, b)| a.to_bits() == b.to_bits())
        })
    })
}
