// SPDX-License-Identifier: LGPL-3.0-or-later

//! Admission and byte/coverage ownership for fixed-arena replay compilation.

use super::*;
use bounded_records::BoundedRecordEncoder;
use spectral_records::StandardRecordScratch;

/// Reconstruction-owned, checked capacities for one compiler attempt.
///
/// The artifact ceiling is an independently admitted storage capacity, not a
/// claimed expansion bound derived from the source sample count. Runtime must
/// reserve [`Self::workspace_bytes`] in addition to its source, weighting, and
/// spill-writer buffers before constructing the compiler.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GriddedNormalCompilationPlan {
    pub(super) binding: LogicalIdentity,
    pub(super) maximum_source_samples: usize,
    pub(super) maximum_correlations: usize,
    pub(super) maximum_spectral_terms: usize,
    pub(super) maximum_native_terms: usize,
    pub(super) maximum_atom_records: usize,
    pub(super) raw_record_capacity: usize,
    pub(super) frame_record_capacity: usize,
    pub(super) maximum_artifact_bytes: u64,
    pub(super) frame_header_bytes: usize,
    pub(super) descriptor_capacity: usize,
    pub(super) diagnostic_capacity: usize,
    transient_workspace_bytes: usize,
    retained_metadata_bytes: usize,
    workspace_bytes: usize,
}

impl GriddedNormalCompilationPlan {
    /// Certify fixed capacities and the descriptor bound of greedy atom packing.
    /// Both record capacities must fit one complete prediction/accumulation atom.
    pub fn new(
        problem: &CompiledProblem,
        maximum_source_samples: usize,
        raw_record_capacity: usize,
        frame_record_capacity: usize,
        maximum_artifact_bytes: u64,
        frame_header_bytes: usize,
    ) -> Result<Self, SpectralOperatorError> {
        let specification = SpectralOperatorSpecification::new(problem)?;
        let (
            maximum_correlations,
            maximum_spectral_terms,
            maximum_native_terms,
            maximum_atom_records,
        ) = compilation_dimensions(problem, &specification)?;
        if maximum_source_samples < maximum_correlations
            || raw_record_capacity < maximum_atom_records
            || frame_record_capacity < maximum_atom_records
            || maximum_artifact_bytes == 0
            || frame_header_bytes == 0
        {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        let layout = GriddedNormalRecordLayout::for_specification(&specification);
        let aw = specification.aw_projection().is_some();
        let diagnostic_capacity = if aw {
            problem
                .inputs()
                .observation_snapshot()
                .sources()
                .iter()
                .try_fold(0_usize, |total, source| {
                    total.checked_add(source.selection().spectral_windows().len())
                })
                .and_then(|windows| windows.checked_mul(layout.normal_moments()))
                .ok_or(SpectralOperatorError::ResidencyOverflow)?
        } else {
            0
        };
        let width = record_bytes(layout, aw)?;
        // Every nonfinal frame contains at least R-a+1 records. The final frame
        // needs one extra descriptor; source/reduction boundaries never flush.
        let minimum_nonfinal_bytes = frame_record_capacity
            .checked_sub(maximum_atom_records)
            .and_then(|records| records.checked_add(1))
            .and_then(|records| records.checked_mul(width))
            .and_then(|bytes| bytes.checked_add(frame_header_bytes))
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        let descriptor_capacity = maximum_artifact_bytes
            .checked_div(minimum_nonfinal_bytes)
            .and_then(|count| count.checked_add(1))
            .and_then(|count| usize::try_from(count).ok())
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        let arenas = BoundedRecordEncoder::workspace_bytes(
            layout,
            aw,
            raw_record_capacity,
            frame_record_capacity,
            maximum_atom_records,
        )?;
        let standard = if aw || matches!(layout, GriddedNormalRecordLayout::Taylor(_)) {
            0
        } else {
            StandardRecordScratch::workspace_bytes(
                maximum_correlations,
                maximum_native_terms,
                maximum_atom_records,
            )?
        };
        let aw_atom = if aw {
            maximum_atom_records.checked_mul(size_of::<ReducedRecordKey>())
        } else {
            Some(0)
        }
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        let native_heap = if standard != 0
            && problem.science().spectral().sampling().kernel()
                == casa_imaging_model::SpectralKernel::Linear
        {
            // Previous native samples may outlive their charged source block.
            // Equal per-correlation projections need not share one allocation.
            let carried_projections =
                casa_imaging_model::SelectedImageDomainProjections::retained_heap_bytes_for_len(
                    specification.chart_count(),
                )
                .and_then(|bytes| bytes.checked_mul(maximum_correlations))
                .ok_or(SpectralOperatorError::ResidencyOverflow)?;
            crate::weighting::native_row_heap_bytes(maximum_correlations, maximum_spectral_terms)
                .map_err(|_| SpectralOperatorError::ResidencyOverflow)?
                .checked_add(carried_projections)
                .ok_or(SpectralOperatorError::ResidencyOverflow)?
        } else {
            0
        };
        let workload = crate::spectral_operator::spectral_operator_workload(
            &specification,
            maximum_source_samples,
            SpectralOperatorPass::InitialMajor,
        )?;
        let gridders = workload
            .convolution_f64_values()
            .checked_mul(size_of::<f64>())
            .and_then(|bytes| {
                specification
                    .compiler_convolution_metadata_bytes()
                    .ok()
                    .and_then(|metadata| bytes.checked_add(metadata))
            })
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        let retained_diagnostics = if specification
            .w_projection()
            .is_some_and(|contract| contract.maximum_abs_w_lambda() > f64::MIN_POSITIVE)
        {
            specification.chart_count()
        } else {
            0
        };
        let retained_diagnostic_bytes = retained_diagnostics
            .checked_mul(size_of::<WProjectionDiagnostics>())
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        let retained_metadata_bytes =
            retained_manifest_bytes(&specification, descriptor_capacity, retained_diagnostics)?;
        let transient_workspace_bytes = [
            arenas,
            standard,
            aw_atom,
            native_heap,
            // Convolution metadata includes the copied diagnostic output, which
            // transfers to the manifest rather than dying with the gridders.
            gridders
                .checked_sub(retained_diagnostic_bytes)
                .ok_or(SpectralOperatorError::ResidencyOverflow)?,
            size_of::<GriddedNormalOperatorCompiler>(),
            diagnostic_capacity
                .checked_mul(size_of::<((u32, usize), WeightingScienceAggregate)>())
                .ok_or(SpectralOperatorError::ResidencyOverflow)?,
        ]
        .into_iter()
        .try_fold(0_usize, |sum, bytes| {
            sum.checked_add(bytes)
                .ok_or(SpectralOperatorError::ResidencyOverflow)
        })?;
        let workspace_bytes = transient_workspace_bytes
            .checked_add(retained_metadata_bytes)
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        Ok(Self {
            binding: static_binding(&specification),
            maximum_source_samples,
            maximum_correlations,
            maximum_spectral_terms,
            maximum_native_terms,
            maximum_atom_records,
            raw_record_capacity,
            frame_record_capacity,
            maximum_artifact_bytes,
            frame_header_bytes,
            descriptor_capacity,
            diagnostic_capacity,
            transient_workspace_bytes,
            retained_metadata_bytes,
            workspace_bytes,
        })
    }

    /// Return the indivisible record bound before choosing workspace capacities.
    pub fn maximum_atom_records(problem: &CompiledProblem) -> Result<usize, SpectralOperatorError> {
        let specification = SpectralOperatorSpecification::new(problem)?;
        Ok(compilation_dimensions(problem, &specification)?.3)
    }

    /// Return simultaneous compiler-owned heap and inline storage, excluding the writer.
    #[must_use]
    pub const fn workspace_bytes(self) -> usize {
        self.workspace_bytes
    }

    /// Return the scientific specification binding certified by this plan.
    #[must_use]
    pub const fn binding(self) -> LogicalIdentity {
        self.binding
    }

    /// Return compiler storage released when the attempt seals or fails.
    #[must_use]
    pub const fn transient_workspace_bytes(self) -> usize {
        self.transient_workspace_bytes
    }

    /// Return manifest allocation capacity retained through the final replay reader.
    /// Includes shared catalogs whose source reservation ends before replay.
    #[must_use]
    pub const fn retained_metadata_bytes(self) -> usize {
        self.retained_metadata_bytes
    }

    /// Return the certified maximum encoded records in a frame.
    #[must_use]
    pub const fn frame_record_capacity(self) -> usize {
        self.frame_record_capacity
    }

    /// Return the fixed raw-record reduction capacity.
    #[must_use]
    pub const fn raw_record_capacity(self) -> usize {
        self.raw_record_capacity
    }

    /// Return the fixed descriptor capacity implied by the admitted byte ceiling.
    #[must_use]
    pub const fn descriptor_capacity(self) -> usize {
        self.descriptor_capacity
    }
}

pub(super) fn retained_manifest_bytes(
    specification: &SpectralOperatorSpecification,
    descriptor_capacity: usize,
    diagnostic_count: usize,
) -> Result<usize, SpectralOperatorError> {
    let specification_bytes = specification
        .owned_heap_bytes()?
        .checked_add(specification.shared_catalog_heap_bytes()?)
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    descriptor_capacity
        .checked_mul(size_of::<BlockDescriptor>())
        .and_then(|bytes| bytes.checked_add(size_of::<GriddedNormalOperatorManifest>()))
        .and_then(|bytes| bytes.checked_add(2 * size_of::<usize>()))
        .and_then(|bytes| {
            diagnostic_count
                .checked_mul(size_of::<WProjectionDiagnostics>())
                .and_then(|diagnostics| bytes.checked_add(diagnostics))
        })
        .and_then(|bytes| bytes.checked_add(specification_bytes))
        .and_then(|bytes| {
            let channels = if matches!(
                GriddedNormalRecordLayout::for_specification(specification),
                GriddedNormalRecordLayout::ChannelLocal { .. }
            ) {
                specification.slab().total_channels()
            } else {
                0
            };
            channels
                .checked_mul(size_of::<std::ops::Range<usize>>())
                .and_then(|support| bytes.checked_add(support))
        })
        .ok_or(SpectralOperatorError::ResidencyOverflow)
}

fn compilation_dimensions(
    problem: &CompiledProblem,
    specification: &SpectralOperatorSpecification,
) -> Result<(usize, usize, usize, usize), SpectralOperatorError> {
    let correlations = problem
        .inputs()
        .observation_snapshot()
        .sources()
        .iter()
        .flat_map(|source| source.selection().correlations())
        .map(|selection| selection.products().len())
        .max()
        .unwrap_or(1)
        .max(1);
    let spectral = crate::weighting::maximum_spectral_terms(problem);
    let native = spectral
        .checked_mul(specification.chart_count())
        .and_then(|terms| terms.checked_mul(specification.polarization_count()))
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let atom = if specification.aw_projection().is_some() {
        specification.chart_count()
    } else if matches!(
        GriddedNormalRecordLayout::for_specification(specification),
        GriddedNormalRecordLayout::Taylor(_)
    ) {
        1
    } else {
        let stencils = spectral
            .checked_mul(2)
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        let stencils = if problem.science().spectral().sampling().kernel()
            == casa_imaging_model::SpectralKernel::Linear
        {
            stencils.max(5)
        } else {
            stencils
        };
        stencils
            .checked_mul(specification.chart_count())
            .and_then(|records| records.checked_mul(specification.polarization_count()))
            .ok_or(SpectralOperatorError::ResidencyOverflow)?
    };
    if native == 0 || atom == 0 {
        return Err(SpectralOperatorError::ResidencyOverflow);
    }
    Ok((correlations, spectral, native, atom))
}

/// A complete frame borrowed only for the duration of one synchronous sink call.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct GriddedNormalOperatorFrame<'a> {
    sequence: u64,
    record_count: u64,
    encoded: &'a [u8],
}

impl<'a> GriddedNormalOperatorFrame<'a> {
    /// Return the zero-based frame sequence, independent of source blocks.
    #[must_use]
    pub const fn sequence(self) -> u64 {
        self.sequence
    }
    /// Return complete fixed-width records in this frame.
    #[must_use]
    pub const fn record_count(self) -> u64 {
        self.record_count
    }
    /// Borrow the private encoding until the sink returns.
    #[must_use]
    pub const fn encoded_bytes(self) -> &'a [u8] {
        self.encoded
    }
}

/// Cumulative work and fixed-capacity evidence owned by the compiler.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GriddedNormalCompilationMeasurements {
    pub source_blocks: u64,
    pub source_samples: u64,
    pub source_cardinality: Option<GriddedNormalSourceCardinality>,
    pub frames: u64,
    pub reduced_groups: u64,
    pub reduced_records: u64,
    pub workspace_bytes: usize,
    pub peak_raw_records: usize,
    pub peak_frame_records: usize,
}

#[derive(Debug)]
pub(super) struct FixedDescriptors {
    pub(super) storage: Box<[BlockDescriptor]>,
    length: usize,
}

impl std::ops::Deref for FixedDescriptors {
    type Target = [BlockDescriptor];
    fn deref(&self) -> &Self::Target {
        &self.storage[..self.length]
    }
}

pub(super) struct CompilationFrames {
    encoder: BoundedRecordEncoder,
    ledger: FrameLedger,
}

struct FrameLedger {
    plan: GriddedNormalCompilationPlan,
    descriptors: FixedDescriptors,
    record_count: u64,
    frame_bytes: u64,
    sink_time: Duration,
    checksum_time: Duration,
    observe: bool,
}

impl CompilationFrames {
    pub(super) fn new(
        plan: GriddedNormalCompilationPlan,
        layout: GriddedNormalRecordLayout,
        aw: bool,
        observe: bool,
    ) -> Result<Self, SpectralOperatorError> {
        let mut encoder = BoundedRecordEncoder::new(
            layout,
            aw,
            plan.raw_record_capacity,
            plan.frame_record_capacity,
            plan.maximum_atom_records,
        )?;
        encoder.observe_timings(observe);
        let mut descriptors = Vec::new();
        descriptors
            .try_reserve_exact(plan.descriptor_capacity)
            .map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
        descriptors.resize(plan.descriptor_capacity, BlockDescriptor::default());
        Ok(Self {
            encoder,
            ledger: FrameLedger {
                plan,
                descriptors: FixedDescriptors {
                    storage: descriptors.into_boxed_slice(),
                    length: 0,
                },
                record_count: 0,
                frame_bytes: 0,
                sink_time: Duration::ZERO,
                checksum_time: Duration::ZERO,
                observe,
            },
        })
    }

    pub(super) fn push_group(
        &mut self,
        records: &[ReducedRecordKey],
        sink: &mut impl FnMut(GriddedNormalOperatorFrame<'_>) -> Result<(), SpectralOperatorError>,
    ) -> Result<(), SpectralOperatorError> {
        self.encoder.push_group(records, &mut |bytes, count| {
            self.ledger.emit(bytes, count, sink)
        })
    }

    pub(super) fn push_taylor(
        &mut self,
        record: TaylorRecordKey,
        sink: &mut impl FnMut(GriddedNormalOperatorFrame<'_>) -> Result<(), SpectralOperatorError>,
    ) -> Result<(), SpectralOperatorError> {
        self.encoder.push_taylor(record, &mut |bytes, count| {
            self.ledger.emit(bytes, count, sink)
        })
    }

    pub(super) fn finish(
        &mut self,
        sink: &mut impl FnMut(GriddedNormalOperatorFrame<'_>) -> Result<(), SpectralOperatorError>,
    ) -> Result<(), SpectralOperatorError> {
        self.encoder
            .finish(&mut |bytes, count| self.ledger.emit(bytes, count, sink))
    }

    pub(super) fn measurements(&self, measurements: &mut GriddedNormalCompilationMeasurements) {
        measurements.frames = self.encoder.flushes();
        measurements.reduced_records = self.ledger.record_count;
        measurements.reduced_groups = self.encoder.reduced_groups();
        measurements.peak_raw_records = self.encoder.peak_raw_records();
        measurements.peak_frame_records = self.encoder.peak_frame_records();
    }

    pub(super) fn timings(&self) -> (GriddedNormalOperatorStageTimings, Duration) {
        let mut timings = self.encoder.timings();
        timings.encoding_checksum += self.ledger.checksum_time;
        (timings, self.ledger.sink_time)
    }

    pub(super) fn into_descriptors(self) -> FixedDescriptors {
        self.ledger.descriptors
    }
}

impl FrameLedger {
    fn emit(
        &mut self,
        encoded: &[u8],
        record_count: u64,
        sink: &mut impl FnMut(GriddedNormalOperatorFrame<'_>) -> Result<(), SpectralOperatorError>,
    ) -> Result<(), SpectralOperatorError> {
        if encoded.is_empty() || record_count == 0 {
            return Err(SpectralOperatorError::InvalidGriddedRecord);
        }
        let frame_bytes = u64::try_from(encoded.len())
            .ok()
            .and_then(|bytes| bytes.checked_add(self.plan.frame_header_bytes as u64))
            .and_then(|bytes| self.frame_bytes.checked_add(bytes))
            .ok_or(SpectralOperatorError::CoverageOverflow)?;
        if self.descriptors.length == self.descriptors.storage.len()
            || frame_bytes > self.plan.maximum_artifact_bytes
        {
            return Err(SpectralOperatorError::GriddedCompilationCapacity);
        }
        let total_records = self
            .record_count
            .checked_add(record_count)
            .ok_or(SpectralOperatorError::CoverageOverflow)?;
        let started = self.observe.then(Instant::now);
        let digest = Sha256::digest(encoded).into();
        if let Some(started) = started {
            self.checksum_time += started.elapsed();
        }
        let started = self.observe.then(Instant::now);
        sink(GriddedNormalOperatorFrame {
            sequence: self.descriptors.length as u64,
            record_count,
            encoded,
        })?;
        if let Some(started) = started {
            self.sink_time += started.elapsed();
        }
        self.descriptors.storage[self.descriptors.length] = BlockDescriptor {
            record_count,
            digest,
        };
        self.descriptors.length += 1;
        self.record_count = total_records;
        self.frame_bytes = frame_bytes;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::OnceLock;

    fn problem() -> &'static CompiledProblem {
        &fixture().0
    }

    fn fixture() -> &'static (CompiledProblem, SelectedObservationGenerationId) {
        static FIXTURE: OnceLock<(CompiledProblem, SelectedObservationGenerationId)> =
            OnceLock::new();
        FIXTURE.get_or_init(|| {
            let cells = 512 * 512;
            let (problem, lifecycle, model, normal) =
                crate::major_cycle::native_minor_fixture::build(
                    vec![Complex64::default(); 2 * cells].into_boxed_slice(),
                    vec![Complex64::default(); 3 * cells].into_boxed_slice(),
                    None,
                );
            let generation = normal.selected_generation();
            drop((lifecycle, model, normal));
            (problem, generation)
        })
    }

    fn dimensions() -> (usize, usize, GriddedNormalRecordLayout, usize) {
        let specification =
            SpectralOperatorSpecification::new(problem()).expect("fixture specification");
        let (correlations, _, _, atom) =
            compilation_dimensions(problem(), &specification).expect("fixture dimensions");
        let layout = GriddedNormalRecordLayout::for_specification(&specification);
        (
            correlations,
            atom,
            layout,
            record_bytes(layout, false).expect("record width"),
        )
    }

    fn key(taps: u64) -> TaylorRecordKey {
        TaylorRecordKey {
            taps,
            frequency_hz: 1.0e9_f64.to_bits(),
            imaging_weight: 1.0_f64.to_bits(),
        }
    }

    #[test]
    fn owner_plan_rejects_incomplete_atoms_correlation_groups_and_overflow() {
        let (correlations, atom, _, _) = dimensions();
        for (block, raw, frame, storage, header) in [
            (correlations, atom - 1, atom, 4096, 72),
            (correlations, atom, atom - 1, 4096, 72),
            (correlations - 1, atom, atom, 4096, 72),
            (correlations, atom, atom, 0, 72),
            (correlations, atom, atom, 4096, 0),
            (correlations, usize::MAX, atom, 4096, 72),
            (correlations, atom, usize::MAX, 4096, 72),
            (correlations, atom, atom, 4096, usize::MAX),
            (correlations, atom, atom, u64::MAX, 1),
        ] {
            assert!(
                matches!(
                    GriddedNormalCompilationPlan::new(
                        problem(),
                        block,
                        raw,
                        frame,
                        storage,
                        header
                    ),
                    Err(SpectralOperatorError::ResidencyOverflow)
                ),
                "accepted B={block} U={raw} R={frame} S={storage} h={header}"
            );
        }
        let minimum = GriddedNormalCompilationPlan::new(problem(), correlations, atom, atom, 1, 1)
            .expect("minimum atom and correlation capacities are admissible");
        assert_eq!(minimum.raw_record_capacity(), atom);
        assert_eq!(minimum.frame_record_capacity(), atom);
        assert_eq!(minimum.descriptor_capacity(), 1);
    }

    #[test]
    fn descriptor_admission_changes_only_at_exact_nonfinal_frame_byte_boundaries() {
        let (correlations, atom, _, width) = dimensions();
        let frame = atom + 2;
        let minimum_nonfinal_bytes = 72 + 3 * width;
        let mut previous = None;
        for (storage, expected) in [
            (minimum_nonfinal_bytes - 1, 1),
            (minimum_nonfinal_bytes, 2),
            (minimum_nonfinal_bytes + 1, 2),
            (2 * minimum_nonfinal_bytes - 1, 2),
            (2 * minimum_nonfinal_bytes, 3),
        ] {
            let plan = GriddedNormalCompilationPlan::new(
                problem(),
                correlations,
                atom,
                frame,
                storage as u64,
                72,
            )
            .expect("bounded storage admission");
            assert_eq!(plan.descriptor_capacity(), expected);
            if let Some((prior_descriptors, prior_bytes)) = previous {
                assert_eq!(
                    plan.workspace_bytes() - prior_bytes,
                    (expected - prior_descriptors) * size_of::<BlockDescriptor>(),
                    "only fixed descriptor storage changes with the byte ceiling"
                );
            }
            previous = Some((expected, plan.workspace_bytes()));
        }
    }

    #[test]
    fn frame_ledger_rejects_storage_overflow_before_calling_sink_or_minting_descriptor() {
        let (correlations, atom, layout, width) = dimensions();
        let frame_bytes = 72 + width;
        for storage in [frame_bytes - 1, frame_bytes] {
            let plan = GriddedNormalCompilationPlan::new(
                problem(),
                correlations,
                atom,
                atom,
                storage as u64,
                72,
            )
            .expect("small explicit storage ceiling");
            let mut frames =
                CompilationFrames::new(plan, layout, false, false).expect("fixed frame workspace");
            let mut emitted = Vec::new();
            let mut sink = |frame: GriddedNormalOperatorFrame<'_>| {
                emitted.push((
                    frame.sequence(),
                    frame.record_count(),
                    frame.encoded_bytes().len(),
                ));
                Ok(())
            };
            frames
                .push_taylor(key(1), &mut sink)
                .expect("buffer one record");
            let result = frames.finish(&mut sink);
            if storage < frame_bytes {
                assert!(matches!(
                    result,
                    Err(SpectralOperatorError::GriddedCompilationCapacity)
                ));
                assert!(matches!(
                    frames.finish(&mut sink),
                    Err(SpectralOperatorError::GriddedCompilationPoisoned)
                ));
                assert!(emitted.is_empty());
                assert_eq!(frames.ledger.descriptors.len(), 0);
                assert_eq!(frames.ledger.record_count, 0);
                assert_eq!(frames.ledger.frame_bytes, 0);
            } else {
                result.expect("exact byte ceiling accepts the frame");
                assert_eq!(emitted, vec![(0, 1, width)]);
                assert_eq!(frames.ledger.descriptors.len(), 1);
                assert_eq!(frames.ledger.record_count, 1);
                assert_eq!(frames.ledger.frame_bytes, frame_bytes as u64);
            }
        }
    }

    #[test]
    fn sealed_empty_program_retains_reserved_metadata_not_only_live_descriptors() {
        let (correlations, atom, _, _) = dimensions();
        let plan = GriddedNormalCompilationPlan::new(problem(), correlations, atom, atom, 4096, 72)
            .expect("compiler plan");
        assert_eq!(
            plan.workspace_bytes(),
            plan.transient_workspace_bytes()
                .checked_add(plan.retained_metadata_bytes())
                .expect("checked split")
        );
        assert!(plan.transient_workspace_bytes() > 0);
        assert!(plan.descriptor_capacity() > 0);
        let mut compiler = GriddedNormalOperatorCompiler::new(
            problem(),
            plan,
            SourceCardinalityObservation::Disabled,
        )
        .expect("compiler");
        compiler
            .finish_rows_and_frames(&mut |_| panic!("empty compiler emitted a frame"))
            .expect("finish empty compiler");
        let weighting_plan = crate::weighting::plan_weighting(
            problem(),
            crate::weighting::WeightingExecutionLimits::new(1, 1).unwrap(),
        )
        .unwrap();
        let density =
            crate::weighting::begin_weighting_generation(problem(), &weighting_plan).unwrap();
        let stream = density
            .finish_into_stream(problem(), &weighting_plan)
            .unwrap();
        let (_, _, replay) = stream.finish().unwrap();
        let program = compiler
            .complete(&replay, fixture().1, None)
            .expect("seal empty program");
        assert_eq!(program.block_count(), 0);
        assert_eq!(program.compilation_binding(), plan.binding());
        assert_eq!(
            program.retained_metadata_bytes(),
            plan.retained_metadata_bytes()
        );
        assert_eq!(
            program.manifest.descriptors.storage.len(),
            plan.descriptor_capacity()
        );
        let without_descriptors = retained_manifest_bytes(
            &program.manifest.specification,
            0,
            program.w_projection_diagnostics().len(),
        )
        .unwrap();
        assert_eq!(
            program.retained_metadata_bytes() - without_descriptors,
            plan.descriptor_capacity() * size_of::<BlockDescriptor>()
        );
        let cloned = program.clone();
        assert!(Arc::ptr_eq(&program.manifest, &cloned.manifest));
        assert_eq!(
            cloned.retained_metadata_bytes(),
            program.retained_metadata_bytes()
        );
    }

    #[test]
    fn failed_frame_sink_cannot_finish_or_seal_the_compiler() {
        let (correlations, atom, _, _) = dimensions();
        let plan = GriddedNormalCompilationPlan::new(problem(), correlations, atom, atom, 4096, 72)
            .expect("compiler plan");
        let mut compiler = GriddedNormalOperatorCompiler::new(
            problem(),
            plan,
            SourceCardinalityObservation::Disabled,
        )
        .expect("compiler");
        let mut calls = 0;
        let mut sink = |_: GriddedNormalOperatorFrame<'_>| {
            calls += 1;
            Err(SpectralOperatorError::DiagnosticStop)
        };
        compiler
            .frames
            .as_mut()
            .expect("active frames")
            .push_taylor(key(1), &mut sink)
            .expect("buffer a frame before compiler finish");
        assert!(matches!(
            compiler.finish_rows_and_frames(&mut sink),
            Err(SpectralOperatorError::DiagnosticStop)
        ));
        assert!(matches!(
            compiler.finish_rows_and_frames(&mut sink),
            Err(SpectralOperatorError::GriddedCompilationPoisoned)
        ));
        assert_eq!(calls, 1);
        let weighting_plan = crate::weighting::plan_weighting(
            problem(),
            crate::weighting::WeightingExecutionLimits::new(1, 1).unwrap(),
        )
        .unwrap();
        let density =
            crate::weighting::begin_weighting_generation(problem(), &weighting_plan).unwrap();
        let stream = density
            .finish_into_stream(problem(), &weighting_plan)
            .unwrap();
        let (_, _, replay) = stream.finish().unwrap();
        assert!(matches!(
            compiler.complete(&replay, fixture().1, None),
            Err(SpectralOperatorError::GriddedCompilationPoisoned)
        ));
    }
}
