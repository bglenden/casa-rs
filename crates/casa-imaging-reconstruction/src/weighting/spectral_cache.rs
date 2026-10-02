// SPDX-License-Identifier: LGPL-3.0-or-later

//! Source-channel-bounded reuse of the canonical spectral stencil compiler.

use casa_imaging_model::{
    CompiledProblem, MeasurementSetIdentity, ReconstructionBasis, SelectedObservationSampleView,
    SelectedSpectralContribution, SelectedSpectralContributions, SelectedSpectralEvaluation,
    SelectedSpectralInterval,
};

use super::{WeightingError, maximum_spectral_terms, smallvec_heap_bytes};

#[derive(Clone, Copy, PartialEq)]
struct SpectralContributionKey {
    measurement_set: MeasurementSetIdentity,
    field_id: i32,
    spectral_window_id: u32,
    channel_index: u32,
    native: SelectedSpectralInterval,
    output_frame: SelectedSpectralInterval,
}

type Slot = Option<(SpectralContributionKey, SelectedSpectralContributions)>;

/// Bounded, problem-bound reuse of exactly equal source spectral intervals.
///
/// There is one direct-mapped slot per channel in the largest selected spectral
/// window. Sparse channel ordinals can collide; the complete key is compared
/// before reuse, and every miss uses the canonical compiler. Storage therefore
/// depends on selected channel count, not row count or the largest channel id.
#[doc(hidden)]
pub struct WeightingSpectralCache<'a> {
    problem: &'a CompiledProblem,
    slots: Box<[Slot]>,
    maximum_terms: usize,
}

impl<'a> WeightingSpectralCache<'a> {
    /// Allocate only after the weighting plan's spectral-cache reservation is held.
    pub fn new(problem: &'a CompiledProblem) -> Result<Self, WeightingError> {
        let slot_count = slot_count(problem);
        let maximum_terms = maximum_spectral_terms(problem);
        workspace_bytes(slot_count, maximum_terms, axis_channels(problem))?;
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(slot_count)
            .map_err(|_| WeightingError::ResidencyOverflow)?;
        slots.resize_with(slot_count, || None);
        Ok(Self {
            problem,
            slots: slots.into_boxed_slice(),
            maximum_terms,
        })
    }

    pub(super) fn planned_bytes(problem: &CompiledProblem) -> Result<usize, WeightingError> {
        workspace_bytes(
            slot_count(problem),
            maximum_spectral_terms(problem),
            axis_channels(problem),
        )
    }

    /// Compile or reuse contributions, without caching sample validity or weights.
    pub fn compile(
        &mut self,
        sample: SelectedObservationSampleView<'_>,
        evaluation: SelectedSpectralEvaluation,
    ) -> Result<SelectedSpectralContributions, WeightingError> {
        let address = sample.address();
        let key = SpectralContributionKey {
            measurement_set: address.measurement_set,
            field_id: sample.metadata().field_id,
            spectral_window_id: address.spectral_window_id,
            channel_index: address.channel_index,
            native: evaluation.native(),
            output_frame: evaluation.output_frame(),
        };
        cached_contributions(&mut self.slots, self.maximum_terms, key, || {
            Ok(
                crate::compile_spectral_stencil(self.problem, sample, evaluation)?
                    .contributions()
                    .clone(),
            )
        })
    }
}

fn slot_count(problem: &CompiledProblem) -> usize {
    problem
        .inputs()
        .observation_snapshot()
        .sources()
        .iter()
        .flat_map(|source| source.selection().spectral_windows())
        .map(|window| window.channel_indices().len())
        .max()
        .unwrap_or(0)
}

fn axis_channels(problem: &CompiledProblem) -> usize {
    match problem.reconstruction().basis() {
        ReconstructionBasis::Constant | ReconstructionBasis::Taylor { .. } => 0,
        ReconstructionBasis::TaylorViaChannelMajor { .. }
        | ReconstructionBasis::ChannelLocal { .. }
        | ReconstructionBasis::JointContinuumLine { .. } => {
            problem.geometry().spectral().output_channels()
        }
    }
}

fn workspace_bytes(
    slots: usize,
    maximum_terms: usize,
    axis_channels: usize,
) -> Result<usize, WeightingError> {
    // A miss can retain the old entry while the compiler receipt and its cloned
    // contribution set coexist. All three use the same declared sparse bound.
    let spilled = smallvec_heap_bytes::<SelectedSpectralContribution>(maximum_terms)?;
    let axis_bytes = if axis_channels == 0 {
        0
    } else {
        axis_channels
            .checked_mul(2)
            .and_then(|values| values.checked_add(1))
            .and_then(|values| values.checked_mul(size_of::<f64>()))
            .ok_or(WeightingError::ResidencyOverflow)?
    };
    slots
        .checked_mul(size_of::<Slot>())
        .and_then(|bytes| bytes.checked_add(size_of::<WeightingSpectralCache<'_>>()))
        .and_then(|bytes| bytes.checked_add(slots.checked_add(2)?.checked_mul(spilled)?))
        .and_then(|bytes| bytes.checked_add(axis_bytes))
        .filter(|bytes| *bytes <= isize::MAX as usize)
        .ok_or(WeightingError::ResidencyOverflow)
}

fn cached_contributions(
    slots: &mut [Slot],
    maximum_terms: usize,
    key: SpectralContributionKey,
    compile: impl FnOnce() -> Result<SelectedSpectralContributions, WeightingError>,
) -> Result<SelectedSpectralContributions, WeightingError> {
    let ordinal = usize::try_from(key.channel_index)
        .ok()
        .and_then(|channel| channel.checked_rem(slots.len()))
        .ok_or(WeightingError::CoverageMismatch)?;
    let slot = &mut slots[ordinal];
    if let Some((cached_key, contributions)) = slot
        && *cached_key == key
    {
        return Ok(contributions.clone());
    }
    let contributions = compile()?;
    if contributions.len() > maximum_terms {
        return Err(crate::SpectralStencilError::PlannerTermBound.into());
    }
    *slot = Some((key, contributions.clone()));
    Ok(contributions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_imaging_model::LogicalIdentity;

    fn key(channel_index: u32) -> SpectralContributionKey {
        SpectralContributionKey {
            measurement_set: MeasurementSetIdentity::new(LogicalIdentity::from_sha256([1; 32])),
            field_id: 0,
            spectral_window_id: 0,
            channel_index,
            native: SelectedSpectralInterval::new(100.0, 90.0, 110.0).unwrap(),
            output_frame: SelectedSpectralInterval::new(100.0, 90.0, 110.0).unwrap(),
        }
    }

    fn contributions(channel: u32) -> SelectedSpectralContributions {
        SelectedSpectralContributions::new([
            SelectedSpectralContribution::new(channel, 1.0, 100.0).unwrap()
        ])
        .unwrap()
    }

    #[test]
    fn repeated_rows_compile_once_per_channel_and_collisions_recompile() {
        let mut slots = [None, None, None, None];
        let mut compilations = 0;
        for _ in 0..100 {
            for channel in 2..6 {
                let actual = cached_contributions(&mut slots, 4, key(channel), || {
                    compilations += 1;
                    Ok(contributions(channel))
                })
                .unwrap();
                assert_eq!(actual, contributions(channel));
            }
        }
        assert_eq!(compilations, 4);
        for channel in [6, 2] {
            assert_eq!(
                cached_contributions(&mut slots, 4, key(channel), || {
                    compilations += 1;
                    Ok(contributions(channel))
                })
                .unwrap(),
                contributions(channel)
            );
        }
        assert_eq!(compilations, 6);
        assert_eq!(slots.len(), 4);
        assert_eq!(
            cached_contributions(&mut slots, 4, key(u32::MAX), || Ok(contributions(0))).unwrap(),
            contributions(0)
        );
        assert_eq!(slots.len(), 4);
    }

    #[test]
    fn every_selection_and_frequency_key_change_invalidates_its_slot() {
        let original = key(0);
        let interval = SelectedSpectralInterval::new(101.0, 91.0, 111.0).unwrap();
        let mut variants = [original; 5];
        variants[0].measurement_set =
            MeasurementSetIdentity::new(LogicalIdentity::from_sha256([2; 32]));
        variants[1].field_id = 1;
        variants[2].spectral_window_id = 1;
        variants[3].native = interval;
        variants[4].output_frame = interval;
        for changed in variants {
            let mut slots = [Some((original, contributions(0)))];
            assert_eq!(
                cached_contributions(&mut slots, 4, changed, || Ok(contributions(1))).unwrap(),
                contributions(1)
            );
            assert_eq!(
                cached_contributions(&mut slots, 4, changed, || panic!("equal key must hit"))
                    .unwrap(),
                contributions(1)
            );
        }
    }

    #[test]
    fn empty_results_are_cached_and_failed_misses_preserve_previous_entries() {
        let mut slots = [Some((key(0), contributions(0)))];
        assert!(
            cached_contributions(&mut slots, 4, key(1), || {
                Err(WeightingError::CoverageMismatch)
            })
            .is_err()
        );
        assert_eq!(
            cached_contributions(&mut slots, 4, key(0), || panic!(
                "failed miss replaced entry"
            ))
            .unwrap(),
            contributions(0)
        );
        cached_contributions(&mut slots, 4, key(1), || {
            Ok(SelectedSpectralContributions::empty())
        })
        .unwrap();
        assert!(
            cached_contributions(&mut slots, 4, key(1), || panic!("empty result must hit"))
                .unwrap()
                .is_empty()
        );
        assert!(cached_contributions(&mut [], 4, key(u32::MAX), || Ok(contributions(0))).is_err());
        assert!(cached_contributions(&mut slots, 0, key(2), || Ok(contributions(0))).is_err());
    }

    #[test]
    fn reservation_counts_slots_and_worst_case_spilled_temporaries() {
        assert_eq!(
            workspace_bytes(4, 4, 0).unwrap(),
            size_of::<WeightingSpectralCache<'_>>() + 4 * size_of::<Slot>()
        );
        assert_eq!(
            workspace_bytes(5, 9, 16).unwrap(),
            size_of::<WeightingSpectralCache<'_>>()
                + 5 * size_of::<Slot>()
                + 7 * 16 * size_of::<SelectedSpectralContribution>()
                + 33 * size_of::<f64>()
        );
        assert!(workspace_bytes(usize::MAX, 1, 0).is_err());
        assert!(workspace_bytes(1, usize::MAX, 0).is_err());
        assert!(workspace_bytes(1, 1, usize::MAX).is_err());
    }
}
