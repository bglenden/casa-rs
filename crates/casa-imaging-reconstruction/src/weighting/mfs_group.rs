// SPDX-License-Identifier: LGPL-3.0-or-later

//! One geometry and weight evaluation per channel group, with compact source
//! correlations. Both initial imaging and normal-program compilation borrow it.

use super::*;

#[doc(hidden)]
#[derive(Debug)]
pub struct MfsWeightingGroup {
    first: WeightingSampleValue,
    correlations: SmallVec<[SelectedObservationRunCorrelation; 4]>,
}

impl MfsWeightingGroup {
    /// Combine a phase-prepared first member with this source-owned complete group.
    /// The numeric source owner has already validated row shape and ordering.
    pub fn new(
        first: WeightingSampleValue,
        correlations: impl IntoIterator<Item = SelectedObservationRunCorrelation>,
    ) -> Result<Self, WeightingError> {
        let mut members = SmallVec::with_capacity(first.selected().correlation_group_size());
        members.extend(correlations);
        let correlations = members;
        if correlations.is_empty()
            || correlations.len() != first.selected().correlation_group_size()
            || first.spectral_values.len() != 1
        {
            return Err(WeightingError::ReturnedBlockMismatch);
        }
        Ok(Self {
            first,
            correlations,
        })
    }

    pub(crate) fn first(&self) -> &WeightingSampleValue {
        &self.first
    }

    /// Number of source correlations represented by this group.
    pub fn len(&self) -> usize {
        self.correlations.len()
    }

    /// Whether the group has no source correlations (always false after construction).
    pub fn is_empty(&self) -> bool {
        self.correlations.is_empty()
    }

    fn visit_members(
        &mut self,
        mut visit: impl FnMut(&WeightingSampleValue) -> Result<(), WeightingError>,
    ) -> Result<(), WeightingError> {
        for (ordinal, correlation) in self.correlations.iter().enumerate() {
            set_member(
                &mut self.first.sample,
                *correlation,
                ordinal,
                self.correlations.len(),
            );
            visit(&self.first)?;
        }
        set_member(
            &mut self.first.sample,
            self.correlations[0],
            0,
            self.correlations.len(),
        );
        Ok(())
    }
}

fn set_member(
    sample: &mut WeightingSelectedSample,
    correlation: SelectedObservationRunCorrelation,
    ordinal: usize,
    count: usize,
) {
    sample.address.correlation_index = correlation.correlation_index;
    sample.address.correlation_type = correlation.correlation_type;
    sample.visibility = correlation.visibility;
    sample.raw_input_weight = correlation.input_weight;
    sample.channel_flag = correlation.channel_flag;
    sample.starts_correlation_group = ordinal == 0;
    sample.ends_correlation_group = ordinal + 1 == count;
}

#[derive(Clone, Copy)]
pub(crate) enum WeightedCorrelationGroup<'a> {
    Samples(&'a [WeightingSampleValue]),
    Mfs(&'a MfsWeightingGroup),
}

impl<'a> WeightedCorrelationGroup<'a> {
    pub(crate) fn first(self) -> Result<&'a WeightingSampleValue, crate::SpectralOperatorError> {
        match self {
            Self::Samples(samples) => samples.first(),
            Self::Mfs(group) => Some(group.first()),
        }
        .ok_or(crate::SpectralOperatorError::InvalidSample)
    }

    pub(crate) fn len(self) -> usize {
        match self {
            Self::Samples(samples) => samples.len(),
            Self::Mfs(group) => group.len(),
        }
    }

    pub(crate) fn correlations(
        self,
    ) -> impl Iterator<Item = SelectedObservationRunCorrelation> + 'a {
        (0..self.len()).map(move |ordinal| match self {
            Self::Mfs(group) => group.correlations[ordinal],
            Self::Samples(samples) => {
                let sample = samples[ordinal].selected();
                SelectedObservationRunCorrelation {
                    correlation_index: sample.address.correlation_index,
                    correlation_type: sample.address.correlation_type,
                    visibility: sample.visibility,
                    channel_flag: sample.channel_flag,
                    parallel_hand_group_flag: sample.parallel_hand_group_flag,
                    input_weight: sample.raw_input_weight,
                }
            }
        })
    }

    pub(crate) fn spectral_values(
        self,
        ordinal: usize,
    ) -> impl Iterator<Item = WeightingSpectralValue> + 'a {
        let sample = match self {
            Self::Mfs(group) => &group.first,
            Self::Samples(samples) => &samples[ordinal],
        };
        sample.spectral_values()
    }
}

fn commit_groups(
    groups: &mut Vec<MfsWeightingGroup>,
    max_samples: usize,
    sequence: &mut u64,
    coverage: &mut CoverageEncoder,
    previous_checkpoint: &mut [u8; 32],
    mut accumulate: impl FnMut(&WeightingSampleValue) -> Result<(), WeightingError>,
) -> Result<WeightingReplayChunk, WeightingError> {
    if groups.is_empty() || groups.iter().map(MfsWeightingGroup::len).sum::<usize>() > max_samples {
        return Err(WeightingError::ReturnedBlockMismatch);
    }
    for group in groups.iter_mut() {
        group.visit_members(|member| {
            accumulate(member)?;
            coverage.push(member);
            Ok(())
        })?;
    }
    let next = sequence
        .checked_add(1)
        .ok_or(WeightingError::BlockCountOverflow)?;
    let mut chunk = WeightingReplayChunk::new(*sequence, Vec::new(), coverage, previous_checkpoint);
    chunk.mfs_groups = std::mem::take(groups);
    *sequence = next;
    Ok(chunk)
}

impl FusedWeightingPhase {
    /// Commit complete numeric groups in source order without correlation expansion.
    #[doc(hidden)]
    pub fn commit_mfs_groups(
        &mut self,
        problem: &CompiledProblem,
        groups: &mut Vec<MfsWeightingGroup>,
    ) -> Result<WeightingReplayChunk, WeightingError> {
        if !self.block.is_empty() || self.pending.is_some() {
            return Err(WeightingError::ReturnedBlockMismatch);
        }
        self.block = Vec::new();
        commit_groups(
            groups,
            self.max_block_samples,
            &mut self.block_sequence,
            &mut self.coverage,
            &mut self.previous_checkpoint,
            |member| self.sum.accumulate_prepared(problem, member),
        )
    }
}

impl WeightingReplayPhase<'_> {
    /// Commit complete numeric groups against the frozen weighting generation.
    #[doc(hidden)]
    pub fn commit_mfs_groups(
        &mut self,
        groups: &mut Vec<MfsWeightingGroup>,
    ) -> Result<WeightingReplayChunk, WeightingError> {
        if !self.block.is_empty() || self.pending.is_some() {
            return Err(WeightingError::ReturnedBlockMismatch);
        }
        self.block = Vec::new();
        commit_groups(
            groups,
            self.max_block_samples,
            &mut self.block_sequence,
            &mut self.coverage,
            &mut self.previous_checkpoint,
            |_| {
                self.sample_count = self
                    .sample_count
                    .checked_add(1)
                    .ok_or(WeightingError::SampleCountOverflow)?;
                Ok(())
            },
        )
    }
}
